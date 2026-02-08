import datetime
import re
import threading
import traceback
import shutil
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

from app import schemas
from app.core.config import settings
from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType
from app.schemas.types import EventType
from app.utils.system import SystemUtils

lock = threading.Lock()

class FileMonitorHandler(FileSystemEventHandler):
    def __init__(self, monpath: str, sync: Any, **kwargs):
        super(FileMonitorHandler, self).__init__(**kwargs)
        self._watch_path = monpath
        self.sync = sync

    def on_created(self, event):
        self.sync.event_handler(event=event, text="创建", mon_path=self._watch_path, event_path=event.src_path)

    def on_moved(self, event):
        self.sync.event_handler(event=event, text="移动", mon_path=self._watch_path, event_path=event.dest_path)

    def on_deleted(self, event):
        self.sync.event_handler(event=event, text="删除", mon_path=self._watch_path, event_path=event.src_path, is_delete=True)


class AvMonitor(_PluginBase):
    # 插件元数据
    plugin_name = "AV实时硬链接测试"
    plugin_desc = "监控目录文件变化，实时硬链接。"
    plugin_icon = "Linkace_C.png"
    plugin_version = "1.0.0"
    plugin_author = "fanxing"
    # 作者主页
    author_url = "https://github.com/fanxing-oai"
    # 插件配置项ID前缀
    plugin_config_prefix = "avmonitor_"
    plugin_order = 4
    auth_level = 1

    # 私有属性
    _scheduler = None
    _observer = []
    _enabled = False
    _notify = False
    _onlyonce = False
    _cron = None
    _monitor_dirs = ""
    _mode = "fast"
    _dirconf: Dict[str, Path] = {}
    _rev_dirconf: Dict[str, Path] = {}
    _synced_folders = set()
    _event = threading.Event()

    def init_plugin(self, config: dict = None):
        self._dirconf = {}
        self._rev_dirconf = {}
        self._synced_folders.clear()
        
        if config:
            self._enabled = config.get("enabled")
            self._notify = config.get("notify")
            self._onlyonce = config.get("onlyonce")
            self._mode = config.get("mode") or "fast"
            self._monitor_dirs = config.get("monitor_dirs") or ""
            self._cron = config.get("cron")

        self.stop_service()

        if self._enabled or self._onlyonce:
            monitor_dirs = self._monitor_dirs.split("\n")
            for mon_path in monitor_dirs:
                if ":" not in mon_path:
                    continue
                paths = mon_path.split(":")
                m_path = paths[0]
                t_path = Path(paths[1])
                self._dirconf[m_path] = t_path
                self._rev_dirconf[str(t_path)] = Path(m_path)

                if self._enabled:
                    for target in [m_path, str(t_path)]:
                        try:
                            observer = PollingObserver(timeout=10) if self._mode == "compatibility" else Observer(timeout=10)
                            self._observer.append(observer)
                            observer.schedule(FileMonitorHandler(target, self), path=target, recursive=True)
                            observer.start()
                            logger.info(f"【AV助手】已启动监控: {target}")
                        except Exception as e:
                            logger.error(f"【AV助手】监控启动失败: {str(e)}")

            if self._onlyonce:
                threading.Thread(target=self.sync_all).start()
                self._onlyonce = False
                self.update_config({"onlyonce": False})

    def event_handler(self, event, mon_path: str, text: str, event_path: str, is_delete: bool = False):
        if is_delete:
            self.__handle_delete(event_path, mon_path)
            return
        if event.is_directory:
            return

        if mon_path in self._dirconf:
            self.__handle_src_file(event_path, mon_path)
        elif mon_path in self._rev_dirconf:
            self.__handle_dest_file(event_path, mon_path)

    def __handle_src_file(self, event_path: str, mon_path: str):
        file_path = Path(event_path)
        av_folder = file_path.parent
        if str(av_folder) in self._synced_folders:
            return
        if any(x in event_path for x in ['/@Recycle/', '/#recycle/', '/.', '/@eaDir']):
            return

        with lock:
            if not av_folder.exists(): return
            files = [f.name.lower() for f in av_folder.iterdir() if f.is_file()]
            has_strm = any(f.endswith('.strm') for f in files)
            has_nfo = any(f.endswith('.nfo') for f in files)
            has_imgs = all(img in files for img in ['poster.jpg', 'fanart.jpg', 'thumb.jpg'])

            if has_strm and has_nfo and has_imgs:
                try:
                    strm_f = next(av_folder.glob('*.strm'))
                    nfo_f = next(av_folder.glob('*.nfo'))
                    if strm_f.stem.lower() == nfo_f.stem.lower():
                        if self._process_forward_link(av_folder, mon_path):
                            self._synced_folders.add(str(av_folder))
                except Exception:
                    pass

    def _process_forward_link(self, av_folder: Path, mon_path: str) -> bool:
        target_base = self._dirconf.get(mon_path)
        av_name = av_folder.name
        # 正则增强：支持 FC2, FC2-PPV 以及常规番号
        match = re.match(r'^([a-zA-Z]+)(?:-PPV)?-?\d+', av_name, re.I)
        first_char = match.group(1)[0].upper() if match else "其他"
        dest_folder = target_base / first_char / av_name
        dest_folder.mkdir(parents=True, exist_ok=True)
        success = True
        for item in av_folder.glob('*'):
            if item.is_file():
                dest_item = dest_folder / item.name
                if not dest_item.exists():
                    res, err = SystemUtils.link(item, dest_item)
                    if res != 0: success = False
        return success

    def __handle_dest_file(self, event_path: str, mon_path: str):
        if not event_path.lower().endswith('.json'):
            return
        dest_file = Path(event_path)
        src_base = self._rev_dirconf.get(mon_path)
        av_name = dest_file.parent.name
        with lock:
            for actor_dir in [d for d in src_base.iterdir() if d.is_dir()]:
                target_av_path = actor_dir / av_name
                if target_av_path.exists():
                    back_link = target_av_path / dest_file.name
                    if not back_link.exists():
                        SystemUtils.link(dest_file, back_link)
                        logger.info(f"【AV助手】JSON反向回传: {av_name}")
                    return

    def __handle_delete(self, event_path: str, mon_path: str):
        if mon_path not in self._dirconf: return
        target_base = self._dirconf.get(mon_path)
        p_name = Path(event_path).name
        if event_path in self._synced_folders:
            self._synced_folders.remove(event_path)
        for sub in [d for d in target_base.iterdir() if d.is_dir()]:
            t_path = sub / p_name
            if t_path.exists():
                if t_path.is_dir():
                    shutil.rmtree(t_path)
                else:
                    t_path.unlink()

    def sync_all(self):
        logger.info("【AV助手】启动全量同步...")
        self._synced_folders.clear()
        for m_path in self._dirconf.keys():
            root = Path(m_path)
            if not root.exists(): continue
            for actor in [d for d in root.iterdir() if d.is_dir()]:
                for av in [d for d in actor.iterdir() if d.is_dir()]:
                    sample = next(av.glob('*.strm'), None)
                    if sample:
                        self.__handle_src_file(str(sample), m_path)
        logger.info("【AV助手】全量同步完成")

    # --- 必须实现的抽象方法部分 ---

    def get_state(self) -> bool:
        return self._enabled

    def get_page(self) -> List[dict]:
        """基类必需实现 1"""
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """基类必需实现 2 (修复您当前的报错)"""
        return [{
            "path": "/sync",
            "endpoint": self.sync_all,
            "methods": ["GET"],
            "summary": "同步任务",
            "description": "手动触发全量同步"
        }]

    def get_command(self) -> List[Dict[str, Any]]:
        """基类必需实现 3"""
        return [{"cmd": "/av_sync", "event": EventType.PluginAction, "desc": "同步AV", "category": "管理", "data": {"action": "sync"}}]

    def get_service(self) -> List[Dict[str, Any]]:
        """基类必需实现 4"""
        if self._enabled and self._cron:
            return [{
                "id": "AvMonitorTask",
                "name": "AV全量同步任务",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.sync_all,
                "kwargs": {}
            }]
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """基类必需实现 5"""
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用插件'}}]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VSwitch', 'props': {'model': 'notify', 'label': '发送通知'}}]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VSwitch', 'props': {'model': 'onlyonce', 'label': '立即全量同步一次'}}]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'mode',
                                            'label': '监控模式',
                                            'items': [
                                                {'title': '兼容模式 (Polling)', 'value': 'compatibility'},
                                                {'title': '性能模式 (Inotify)', 'value': 'fast'}
                                            ]
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {'model': 'cron', 'label': '定时周期', 'placeholder': '5位cron'}
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {'model': 'monitor_dirs', 'label': '映射关系', 'placeholder': '/media/9kg:/media/番号系列'}
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "notify": False,
            "onlyonce": False,
            "mode": "fast",
            "monitor_dirs": "",
            "cron": ""
        }

    @eventmanager.register(EventType.PluginAction)
    def remote_action(self, event: Event):
        if event.event_data.get("action") == "sync":
            self.sync_all()

    def stop_service(self):
        if self._observer:
            for obs in self._observer:
                try: obs.stop(); obs.join()
                except: pass
        self._observer = []
        if self._scheduler:
            self._scheduler.shutdown()
            self._scheduler = None