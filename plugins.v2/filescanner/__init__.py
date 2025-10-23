import json
import logging
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, List, Dict, Tuple, Optional

from apscheduler.triggers.cron import CronTrigger
from fastapi import Response
from sqlalchemy import text

from app import schemas
from app.chain.transfer import TransferChain
from app.core.event import eventmanager, EventType
from app.db.transferhistory_oper import TransferHistoryOper
from app.log import logger, LoggerManager, log_settings
from app.plugins import _PluginBase
from app.schemas import NotificationType


class FileScanner(_PluginBase):
    """
    文件扫描整理插件
    """
    # 插件名称
    plugin_name = "文件扫描整理"
    # 插件描述
    plugin_desc = "定时扫描指定目录并自动整理文件到目标存储，支持转移历史清理"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/penwyp/MoviePilot-Plugins/main/icons/Filerun_A.png"
    # 插件版本
    plugin_version = "3.3"
    # 插件作者
    plugin_author = "penwyp"
    # 作者主页
    author_url = "https://github.com/penwyp/MoviePilot-Plugins"
    # 插件配置项ID前缀
    plugin_config_prefix = "filescanner_"
    # 加载顺序
    plugin_order = 20
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _enabled = False
    _tasks = []
    _notify = False
    _transfer_chain = None
    _process_delay = 2  # 文件处理间隔延时（秒）
    _batch_size = 10   # 批量处理大小限制
    _max_retries = 3   # 最大重试次数
    _skip_processed = True  # 跳过已整理文件
    _debug_scheduler = False  # 调试模式
    _scheduler_logger = None  # 独立的调度器日志记录器
    _task_status = {}  # 任务执行状态记录

    def init_plugin(self, config: dict = None):
        """
        初始化插件 - 增强版本
        """
        self._log_scheduler("info", "开始初始化 FileScanner 插件")
        
        if not config:
            self._log_scheduler("warning", "插件配置为空，使用默认配置")
            return
        
        # 配置参数初始化
        self._enabled = config.get("enabled", False)
        self._notify = config.get("notify", False)
        self._process_delay = config.get("process_delay", 2)
        self._batch_size = config.get("batch_size", 10)
        self._max_retries = config.get("max_retries", 3)
        self._skip_processed = config.get("skip_processed", True)
        self._debug_scheduler = config.get("debug_scheduler", False)
        
        self._log_scheduler("info", f"插件配置: enabled={self._enabled}, notify={self._notify}, debug={self._debug_scheduler}")
        
        # 初始化独立的调度器日志记录器（优先初始化，以便后续日志记录）
        try:
            self._init_scheduler_logger()
        except Exception as e:
            logger.error(f"初始化调度器日志记录器失败: {str(e)}")
            # 使用默认日志器
            self._scheduler_logger = logger
        
        # 解析任务列表
        tasks_json = config.get("tasks", "[]")
        try:
            self._tasks = json.loads(tasks_json) if isinstance(tasks_json, str) else tasks_json
            self._log_scheduler("info", f"成功解析任务配置，共 {len(self._tasks)} 个任务")
            
            # 验证任务配置
            valid_count = 0
            for idx, task in enumerate(self._tasks):
                if not isinstance(task, dict):
                    self._log_scheduler("warning", f"任务 {idx} 配置无效：不是字典类型")
                    continue
                
                # 验证必需字段
                required_fields = ['source_path', 'target_storage', 'target_path']
                missing_fields = [field for field in required_fields if not task.get(field)]
                if missing_fields:
                    self._log_scheduler("warning", f"任务 {idx} 缺少必需字段: {missing_fields}")
                    continue
                
                valid_count += 1
                self._log_scheduler("debug", f"任务 {idx} 验证通过: {task.get('name', f'任务{idx+1}')}")
            
            self._log_scheduler("info", f"有效任务数量: {valid_count}/{len(self._tasks)}")
            
        except Exception as e:
            error_msg = f"解析任务配置失败: {str(e)}"
            logger.error(error_msg)
            self._log_scheduler("error", error_msg)
            self._tasks = []
        
        # 初始化传输链
        if self._enabled:
            try:
                self._transfer_chain = TransferChain()
                self._log_scheduler("info", "传输链初始化成功")
            except Exception as e:
                error_msg = f"传输链初始化失败: {str(e)}"
                logger.error(error_msg)
                self._log_scheduler("error", error_msg)
                # 不中断插件初始化，但在任务执行时会重新尝试初始化
        
        # 恢复任务状态
        try:
            if self._recover_task_status():
                self._log_scheduler("info", "任务状态恢复完成")
            else:
                self._log_scheduler("debug", "没有需要恢复的任务状态")
        except Exception as e:
            self._log_scheduler("warning", f"恢复任务状态失败: {str(e)}")
        
        self._log_scheduler("info", "FileScanner 插件初始化完成")
        
        # 如果启用了调试模式，输出详细配置
        if self._debug_scheduler:
            self._log_scheduler("debug", f"完整配置: {json.dumps(config, ensure_ascii=False, indent=2)}")

    def get_state(self) -> bool:
        """
        获取插件运行状态
        """
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        定义远程控制命令
        """
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """
        获取插件API
        """
        return [
            {
                "path": "/execute_task",
                "endpoint": self.execute_task_api,
                "methods": ["GET", "POST"],
                "auth": "apikey",
                "summary": "立即执行指定任务",
                "description": "手动触发单个扫描整理任务"
            },
            {
                "path": "/dashboard",
                "endpoint": self.dashboard_api,
                "methods": ["GET"],
                "allow_anonymous": True,
                "summary": "FileScanner 控制面板",
                "description": "用户友好的任务管理界面"
            },
            {
                "path": "/downloader_tasks",
                "endpoint": self.downloader_tasks_api,
                "methods": ["GET"],
                "auth": "apikey",
                "summary": "获取下载任务列表",
                "description": "获取下载任务列表，支持状态过滤和分页。参数：status(all/downloading/completed), page(页码), count(每页数量)"
            },
            {
                "path": "/task_status",
                "endpoint": self.task_status_api,
                "methods": ["GET"],
                "auth": "apikey",
                "summary": "获取任务执行状态",
                "description": "获取所有定时任务的执行状态、历史记录和下次执行时间"
            }
        ]

    def get_form(self) -> Tuple[Optional[List[dict]], Dict[str, Any]]:
        """
        拼装插件配置页面，使用Vuetify组件
        """
        return [
            {
                'component': 'VForm',
                'props': {
                    'model': 'plugin_form',
                },
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                            'color': 'primary'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 8
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'notify',
                                            'label': '发送通知',
                                            'color': 'primary'
                                        }
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
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'process_delay',
                                            'label': '处理延时（秒）',
                                            'type': 'number',
                                            'min': 0,
                                            'max': 60,
                                            'hint': '文件处理间隔时间，避免API频率限制'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'batch_size',
                                            'label': '批量大小限制',
                                            'type': 'number',
                                            'min': 1,
                                            'max': 100,
                                            'hint': '单次处理的最大文件数量'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'max_retries',
                                            'label': '最大重试次数',
                                            'type': 'number',
                                            'min': 0,
                                            'max': 10,
                                            'hint': '遇到错误时的重试次数'
                                        }
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
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'skip_processed',
                                            'label': '跳过已整理文件',
                                            'color': 'primary',
                                            'hint': '启用后将跳过已成功整理的文件，避免重复处理和日志输出'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'debug_scheduler',
                                            'label': '调度器调试模式',
                                            'color': 'warning',
                                            'hint': '启用后将输出详细的定时任务执行日志，便于排查问题'
                                        }
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
                                'props': {
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'tasks',
                                            'label': '扫描任务配置',
                                            'rows': 15,
                                            'placeholder': '''[
  {
    "name": "整理电视剧",
    "enabled": true,
    "cron": "0 3 * * *",
    "source_path": "/volume2/pt/电视剧/",
    "target_storage": "local",
    "target_path": "/媒体资源库/电视剧/",
    "transfer_type": "copy",
    "min_filesize": 0,
    "scrape": true,
    "library_category_folder": true,
    "library_type_folder": true
  }
]''',
                                            'hint': 'JSON格式的任务配置，支持多个任务'
                                        }
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
                                'props': {
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'text': '任务配置说明：',
                                            'variant': 'tonal'
                                        },
                                        'content': [
                                            {
                                                'component': 'div',
                                                'html': '''
<b>必填参数：</b><br>
• name: 任务名称<br>
• source_path: 源路径<br>
• target_storage: 目标存储<br>
• target_path: 目标路径<br>
<br>
<b>可选参数：</b><br>
• enabled: 是否启用 (默认 true)<br>
• cron: CRON表达式，定时执行时间 (默认 '0 3 * * *')<br>
• transfer_type: 整理方式 (copy/move/link/softlink，默认 copy)<br>
• min_filesize: 最小文件大小，单位MB (默认 0)<br>
• scrape: 是否刮削 (默认 true)<br>
• library_category_folder: 媒体库类别子目录 (默认 true)<br>
• library_type_folder: 媒体库类型子目录 (默认 true)
'''
                                            }
                                        ]
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
            "process_delay": 2,
            "batch_size": 10,
            "max_retries": 3,
            "skip_processed": True,
            "debug_scheduler": False,
            "tasks": json.dumps([{
                "name": "示例任务",
                "enabled": True,
                "cron": "0 3 * * *",
                "source_path": "/path/to/source/",
                "target_storage": "local",
                "target_path": "/path/to/target/",
                "transfer_type": "copy",
                "min_filesize": 0,
                "scrape": True,
                "library_category_folder": True,
                "library_type_folder": True
            }], indent=2, ensure_ascii=False)
        }

    def get_page(self) -> Optional[List[dict]]:
        """
        拼装插件详情页面
        """
        if not self._tasks:
            return [
                {
                    'component': 'VRow',
                    'content': [
                        {
                            'component': 'VCol',
                            'props': {
                                'cols': 12
                            },
                            'content': [
                                {
                                    'component': 'VAlert',
                                    'props': {
                                        'type': 'info',
                                        'text': '暂未配置任何扫描任务'
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        
        # 显示任务列表
        task_cards = []
        for idx, task in enumerate(self._tasks):
            if not isinstance(task, dict):
                continue
                
            task_cards.append({
                'component': 'VCol',
                'props': {
                    'cols': 12,
                    'md': 6,
                    'lg': 4
                },
                'content': [
                    {
                        'component': 'VCard',
                        'props': {
                            'variant': 'tonal'
                        },
                        'content': [
                            {
                                'component': 'VCardTitle',
                                'props': {
                                    'class': 'd-flex justify-space-between align-center'
                                },
                                'content': [
                                    {
                                        'component': 'span',
                                        'text': task.get('name', f'任务{idx+1}')
                                    },
                                    {
                                        'component': 'VChip',
                                        'props': {
                                            'color': 'success' if task.get('enabled', True) else 'error',
                                            'size': 'small'
                                        },
                                        'text': '启用' if task.get('enabled', True) else '禁用'
                                    }
                                ]
                            },
                            {
                                'component': 'VCardText',
                                'content': [
                                    {
                                        'component': 'VList',
                                        'props': {
                                            'density': 'compact'
                                        },
                                        'content': [
                                            {
                                                'component': 'VListItem',
                                                'props': {
                                                    'prepend-icon': 'mdi-folder-open',
                                                    'title': '源路径',
                                                    'subtitle': task.get('source_path', '未设置')
                                                }
                                            },
                                            {
                                                'component': 'VListItem',
                                                'props': {
                                                    'prepend-icon': 'mdi-folder-move',
                                                    'title': '目标路径',
                                                    'subtitle': f"{task.get('target_storage', 'local')}:{task.get('target_path', '未设置')}"
                                                }
                                            },
                                            {
                                                'component': 'VListItem',
                                                'props': {
                                                    'prepend-icon': 'mdi-transfer',
                                                    'title': '整理方式',
                                                    'subtitle': task.get('transfer_type', 'copy')
                                                }
                                            },
                                            {
                                                'component': 'VListItem',
                                                'props': {
                                                    'prepend-icon': 'mdi-clock-outline',
                                                    'title': '定时执行',
                                                    'subtitle': task.get('cron', '0 3 * * *')
                                                }
                                            }
                                        ]
                                    }
                                ]
                            },
                            {
                                'component': 'VCardActions',
                                'props': {
                                    'class': 'pa-3'
                                },
                                'content': [
                                    {
                                        'component': 'VBtn',
                                        'props': {
                                            'color': 'primary',
                                            'variant': 'tonal',
                                            'size': 'small',
                                            'prepend-icon': 'mdi-play-circle',
                                            'disabled': not task.get('enabled', True),
                                            'href': f'/api/v1/plugin/FileScanner/execute_task?task_index={idx}&action=execute&apikey=API_TOKEN_PLACEHOLDER',
                                            'target': '_blank',
                                            'class': 'mr-2'
                                        },
                                        'text': '立即整理'
                                    },
                                    {
                                        'component': 'VChip',
                                        'props': {
                                            'size': 'small',
                                            'variant': 'text',
                                            'prepend-icon': 'mdi-numeric',
                                            'color': 'grey'
                                        },
                                        'text': f'索引: {idx}'
                                    }
                                ]
                            }
                        ]
                    }
                ]
            })
        
        return [
            {
                'component': 'VRow',
                'content': task_cards
            },
            {
                'component': 'VRow',
                'content': [
                    {
                        'component': 'VCol',
                        'props': {
                            'cols': 12
                        },
                        'content': [
                            {
                                'component': 'VAlert',
                                'props': {
                                    'type': 'info',
                                    'variant': 'tonal'
                                },
                                'content': [
                                    {
                                        'component': 'div',
                                        'text': f'通知设置：{"开启" if self._notify else "关闭"}'
                                    },
                                    {
                                        'component': 'div',
                                        'text': f'处理延时：{self._process_delay}秒'
                                    },
                                    {
                                        'component': 'div',
                                        'text': f'批量大小：{self._batch_size}个文件'
                                    },
                                    {
                                        'component': 'div',
                                        'text': f'重试次数：{self._max_retries}次'
                                    },
                                    {
                                        'component': 'div',
                                        'text': f'跳过已整理：{"开启" if self._skip_processed else "关闭"}'
                                    }
                                ]
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
                        'props': {
                            'cols': 12,
                            'class': 'text-center my-4'
                        },
                        'content': [
                            {
                                'component': 'VBtn',
                                'props': {
                                    'color': 'info',
                                    'variant': 'tonal',
                                    'size': 'large',
                                    'prepend-icon': 'mdi-information',
                                    'href': '/api/v1/plugin/FileScanner/execute_task?apikey=API_TOKEN_PLACEHOLDER',
                                    'target': '_blank'
                                },
                                'text': '查看任务概要'
                            }
                        ]
                    }
                ]
            },
            {
                'component': 'div',
                'html': '''
<script>
// 页面加载时替换所有按钮中的API Token占位符
document.addEventListener('DOMContentLoaded', function() {
    // 从localStorage获取API Token
    const apiToken = localStorage.getItem('api_token') || sessionStorage.getItem('api_token');
    
    if (!apiToken) {
        console.warn('未找到API Token，请确保已登录系统');
        return;
    }
    
    // 替换所有按钮的href中的API_TOKEN_PLACEHOLDER
    const buttons = document.querySelectorAll('a[href*="API_TOKEN_PLACEHOLDER"]');
    buttons.forEach(button => {
        const href = button.getAttribute('href');
        if (href) {
            button.setAttribute('href', href.replace('API_TOKEN_PLACEHOLDER', apiToken));
        }
    });
    
    console.log(`已为 ${buttons.length} 个按钮设置API Token`);
});

// 添加一个执行所有任务的便捷函数
function executeAllTasks() {
    const apiToken = localStorage.getItem('api_token') || sessionStorage.getItem('api_token');
    
    if (!apiToken) {
        alert('未找到API Token，请确保已登录系统');
        return;
    }
    
    const url = `/api/v1/plugin/FileScanner/execute_task?action=execute&apikey=${apiToken}`;
    window.open(url, '_blank');
}
</script>
                '''
            }
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务
        [{
            "id": "服务ID",
            "name": "服务名称",
            "trigger": "触发器：cron/interval/date/CronTrigger.from_crontab()",
            "func": self.xxx,
            "kwargs": {} # 定时器参数
        }]
        """
        # 确保日志记录器已初始化
        if not self._scheduler_logger:
            self._init_scheduler_logger()
            
        if not self._enabled:
            self._log_scheduler("warning", "插件未启用，跳过服务注册")
            return []
            
        if not self._tasks:
            self._log_scheduler("warning", "无任务配置，跳过服务注册")
            return []
        
        services = []
        
        self._log_scheduler("info", f"开始注册定时服务，共有 {len(self._tasks)} 个任务")
        
        # 验证任务配置的有效性
        valid_tasks = []
        for idx, task in enumerate(self._tasks):
            if not isinstance(task, dict):
                self._log_scheduler("error", f"任务 {idx} 配置无效：不是字典类型")
                continue
                
            if not task.get('enabled', True):
                self._log_scheduler("info", f"任务 {idx} 已禁用，跳过注册")
                continue
                
            # 验证必需字段
            required_fields = ['source_path', 'target_storage', 'target_path']
            missing_fields = [field for field in required_fields if not task.get(field)]
            if missing_fields:
                self._log_scheduler("error", f"任务 {idx} 缺少必需字段: {missing_fields}")
                continue
                
            # 验证cron表达式
            task_cron = task.get('cron', '0 3 * * *')
            try:
                # 尝试解析cron表达式
                CronTrigger.from_crontab(task_cron)
            except Exception as e:
                self._log_scheduler("error", f"任务 {idx} 的cron表达式无效: {task_cron}, 错误: {str(e)}")
                # 使用默认的cron表达式
                task_cron = '0 3 * * *'
                task['cron'] = task_cron
                self._log_scheduler("warning", f"任务 {idx} 使用默认cron表达式: {task_cron}")
            
            valid_tasks.append((idx, task))
        
        if not valid_tasks:
            self._log_scheduler("warning", "没有有效的任务配置，跳过服务注册")
            return []
        
        # 为每个有效的任务创建独立的定时服务
        for idx, task in valid_tasks:
            task_cron = task.get('cron', '0 3 * * *')
            task_name = task.get('name', f'任务{idx+1}')
            
            # 创建服务配置
            service_info = {
                "id": f"FileScanner_Task_{idx}",
                "name": f"文件扫描整理 - {task_name}",
                "trigger": CronTrigger.from_crontab(task_cron),
                "func": self.scan_and_transfer_single_task,
                "kwargs": {"task_index": idx}
            }
            
            services.append(service_info)
            
            # 记录服务注册信息
            self._log_scheduler("info", f"成功注册定时任务: {task_name}")
            self._log_scheduler("info", f"  - 任务ID: {service_info['id']}")
            self._log_scheduler("info", f"  - CRON表达式: {task_cron}")
            self._log_scheduler("info", f"  - 源路径: {task.get('source_path')}")
            self._log_scheduler("info", f"  - 目标路径: {task.get('target_storage')}:{task.get('target_path')}")
            
            # 初始化任务状态
            self._init_task_status(idx)
        
        self._log_scheduler("info", f"服务注册完成，共注册 {len(services)} 个定时任务")
        
        # 记录整体状态
        if services:
            self._log_scheduler("info", f"定时任务服务注册成功，等待调度器调度执行")
            # 强制刷新调度器（如果支持）
            try:
                from app.scheduler import scheduler
                if hasattr(scheduler, 'refresh'):
                    scheduler.refresh()
                    self._log_scheduler("debug", "调度器已刷新")
            except Exception as e:
                self._log_scheduler("debug", f"刷新调度器失败（非关键错误）: {str(e)}")
        
        return services

    def scan_and_transfer_single_task(self, task_index: int = None, **kwargs):
        """
        执行单个扫描整理任务 - 增强版本
        """
        start_time = datetime.now()
        
        # 确保日志记录器已初始化
        if not self._scheduler_logger:
            self._init_scheduler_logger()
        
        # 记录任务触发
        self._log_scheduler("info", "=" * 60)
        self._log_scheduler("info", f"定时任务触发 - Task Index: {task_index}")
        self._log_scheduler("debug", f"触发参数: {kwargs}")
        self._log_scheduler("debug", f"插件状态: enabled={self._enabled}, tasks_count={len(self._tasks) if self._tasks else 0}")
        
        # 参数验证
        if task_index is None:
            error_msg = "文件扫描整理：task_index 参数不能为空"
            logger.error(error_msg)
            self._log_scheduler("error", error_msg)
            return
            
        # 插件状态检查
        if not self._enabled:
            self._log_scheduler("warning", "插件未启用，跳过任务执行")
            return
            
        # 任务列表验证
        if not self._tasks or task_index >= len(self._tasks):
            error_msg = f"文件扫描整理：任务索引 {task_index} 无效，当前任务总数: {len(self._tasks) if self._tasks else 0}"
            logger.warning(error_msg)
            self._log_scheduler("warning", error_msg)
            return
            
        # 获取任务配置
        task = self._tasks[task_index]
        if not isinstance(task, dict):
            error_msg = f"任务 {task_index} 配置格式错误：期望dict，实际{type(task)}"
            logger.error(error_msg)
            self._log_scheduler("error", error_msg)
            return
            
        if not task.get('enabled', True):
            self._log_scheduler("info", f"任务 {task_index} 已禁用，跳过执行")
            return
        
        task_name = task.get('name', f'任务{task_index+1}')
        logger.info(f"开始执行定时任务: {task_name}")
        
        # 记录任务开始
        self._log_scheduler("info", f"开始执行任务: {task_name}")
        self._log_scheduler("debug", f"任务完整配置: {json.dumps(task, ensure_ascii=False, indent=2)}")
        
        # 验证任务配置完整性
        required_fields = ['source_path', 'target_storage', 'target_path']
        missing_fields = [field for field in required_fields if not task.get(field)]
        if missing_fields:
            error_msg = f"任务 [{task_name}] 缺少必需字段: {missing_fields}"
            logger.error(error_msg)
            self._log_scheduler("error", error_msg)
            
            # 更新任务状态为失败
            self._update_task_status(task_index, {
                "last_run": start_time.isoformat(),
                "last_status": "failed",
                "last_message": f"配置错误：缺少字段 {missing_fields}"
            })
            return
        
        # 初始化传输链（如果尚未初始化）
        if not self._transfer_chain:
            try:
                self._transfer_chain = TransferChain()
                self._log_scheduler("info", "传输链初始化成功")
            except Exception as e:
                error_msg = f"传输链初始化失败: {str(e)}"
                logger.error(error_msg)
                self._log_scheduler("error", error_msg)
                
                self._update_task_status(task_index, {
                    "last_run": start_time.isoformat(),
                    "last_status": "failed",
                    "last_message": f"传输链初始化失败: {str(e)}"
                })
                return
        
        messages = []
        
        try:
            # 延迟执行SQL清理操作，避免影响任务执行
            self._log_scheduler("debug", "准备执行SQL清理操作")
            self._execute_cleanup_sql()
            
            # 检查是否跳过已整理文件
            if self._skip_processed and self._is_task_processed(task):
                skip_msg = f"任务 [{task_name}] 已整理过，跳过处理"
                messages.append(f"⏭️ {task_name}: 已整理过，跳过")
                logger.info(skip_msg)
                self._log_scheduler("info", skip_msg)
                
                # 更新任务状态
                self._update_task_status(task_index, {
                    "last_run": start_time.isoformat(),
                    "skip_count": self._task_status.get(f"task_{task_index}", {}).get("skip_count", 0) + 1,
                    "last_status": "skipped",
                    "last_message": "已整理过，跳过处理"
                })
                
                if self._notify:
                    self.post_message(
                        mtype=NotificationType.Plugin,
                        title=f"定时任务跳过: {task_name}",
                        text="该任务的文件已整理过，跳过处理"
                    )
                
                # 记录任务结束
                end_time = datetime.now()
                elapsed_time = (end_time - start_time).total_seconds()
                self._log_scheduler("info", f"任务结束 - 状态: 跳过, 耗时: {elapsed_time:.2f}秒")
                self._log_scheduler("info", "=" * 60)
                return
            
            # 执行单个任务
            self._log_scheduler("info", "开始执行文件整理...")
            success = self._execute_single_task(task, task_name, messages)
            
            # 更新任务状态
            status_update = {
                "last_run": start_time.isoformat(),
                "last_status": "success" if success else "failed",
                "last_message": messages[-1] if messages else ""
            }
            
            if success:
                status_update["success_count"] = self._task_status.get(f"task_{task_index}", {}).get("success_count", 0) + 1
            else:
                status_update["fail_count"] = self._task_status.get(f"task_{task_index}", {}).get("fail_count", 0) + 1
                
            self._update_task_status(task_index, status_update)
            
            # 记录任务结束
            end_time = datetime.now()
            elapsed_time = (end_time - start_time).total_seconds()
            self._log_scheduler("info", f"任务执行结果: {'成功' if success else '失败'}")
            if messages:
                self._log_scheduler("info", f"执行消息: {messages}")
            self._log_scheduler("info", f"任务结束 - 状态: {'成功' if success else '失败'}, 耗时: {elapsed_time:.2f}秒")
            self._log_scheduler("info", "=" * 60)
            
            # 发送通知
            if self._notify and messages:
                title = f"定时任务完成: {task_name}"
                text = "\n".join(messages)
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title=title,
                    text=text
                )
                
        except Exception as e:
            error_msg = f"执行任务 [{task_name}] 时发生未预期异常: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self._log_scheduler("error", error_msg)
            
            # 更新任务状态为失败
            self._update_task_status(task_index, {
                "last_run": start_time.isoformat(),
                "last_status": "failed",
                "last_message": f"执行异常: {str(e)}"
            })
            
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title=f"定时任务异常: {task_name}",
                    text=f"任务执行过程中发生异常: {str(e)}"
                )

    def scan_and_transfer_task(self):
        """
        扫描文件并执行整理任务（保留用于向后兼容）
        """
        if not self._enabled:
            return
        
        if not self._tasks:
            logger.warning("文件扫描整理：未配置任何任务")
            return
        
        logger.info("开始执行文件扫描整理任务...")
        
        # 执行SQL清理操作（静默执行，只记录日志）
        self._execute_cleanup_sql()
        
        # 统计信息
        total_tasks = 0
        success_tasks = 0
        failed_tasks = 0
        skipped_tasks = 0  # 跳过的任务数
        messages = []
        
        for task in self._tasks:
            if not isinstance(task, dict):
                continue
            
            # 检查任务是否启用
            if not task.get('enabled', True):
                continue
            
            task_name = task.get('name', '未命名任务')
            total_tasks += 1
            
            # 检查是否跳过已整理文件
            if self._skip_processed and self._is_task_processed(task):
                skipped_tasks += 1
                messages.append(f"⏭️ {task_name}: 已整理过，跳过")
                logger.info(f"任务 [{task_name}] 已整理过，跳过处理")
                continue
            
            # 执行单个任务
            success = self._execute_single_task(task, task_name, messages)
            if success:
                success_tasks += 1
            else:
                failed_tasks += 1
        
        # 汇总结果
        logger.info(f"文件扫描整理任务完成: 总计 {total_tasks} 个, 成功 {success_tasks} 个, 失败 {failed_tasks} 个, 跳过 {skipped_tasks} 个")
        
        # 发送通知
        if self._notify and messages:
            title = "文件扫描整理完成"
            text = f"总计: {total_tasks} | 成功: {success_tasks} | 失败: {failed_tasks} | 跳过: {skipped_tasks}\n\n"
            text += "\n".join(messages)
            
            self.post_message(
                mtype=NotificationType.Plugin,
                title=title,
                text=text
            )

    def _execute_single_task(self, task: dict, task_name: str, messages: list) -> bool:
        """
        执行单个任务，带重试机制
        """
        try:
            # 验证必需参数
            source_path = task.get('source_path')
            target_storage = task.get('target_storage')
            target_path = task.get('target_path')
            
            self._log_scheduler("debug", f"执行任务参数检查 - 任务: {task_name}")
            self._log_scheduler("debug", f"  源路径: {source_path}")
            self._log_scheduler("debug", f"  目标存储: {target_storage}")
            self._log_scheduler("debug", f"  目标路径: {target_path}")
            
            if not all([source_path, target_storage, target_path]):
                error_msg = f"任务 [{task_name}] 缺少必需参数"
                logger.error(error_msg)
                self._log_scheduler("error", error_msg)
                messages.append(f"❌ {task_name}: 配置不完整")
                return False
            
            # 检查存储连接状态
            self._log_scheduler("info", f"检查存储连接状态: {target_storage}")
            if not self._check_storage_connection(target_storage):
                error_msg = f"任务 [{task_name}] 目标存储 {target_storage} 连接失败"
                logger.error(error_msg)
                self._log_scheduler("error", error_msg)
                messages.append(f"❌ {task_name}: 存储连接失败")
                return False
            
            # 构建 FileItem
            fileitem = schemas.FileItem(
                storage="local",
                type="dir",
                path=source_path,
                name=Path(source_path).name
            )
            
            # 获取配置参数
            transfer_type = task.get('transfer_type', 'copy')
            min_filesize = task.get('min_filesize', 0)
            scrape = task.get('scrape', True)
            library_category_folder = task.get('library_category_folder', True)
            library_type_folder = task.get('library_type_folder', True)
            
            self._log_scheduler("info", f"任务配置详情:")
            self._log_scheduler("info", f"  整理方式: {transfer_type}")
            self._log_scheduler("info", f"  最小文件大小: {min_filesize} MB")
            self._log_scheduler("info", f"  刮削信息: {scrape}")
            self._log_scheduler("info", f"  媒体库分类目录: {library_category_folder}")
            self._log_scheduler("info", f"  媒体库类型目录: {library_type_folder}")
            
            logger.info(f"执行任务 [{task_name}]: {source_path} -> {target_storage}:{target_path}")
            self._log_scheduler("info", f"开始执行文件整理: {source_path} -> {target_storage}:{target_path}")
            
            # 重试机制
            for retry_count in range(self._max_retries + 1):
                try:
                    # 添加延时避免API频率限制
                    if retry_count > 0:
                        wait_time = self._process_delay * (retry_count + 1)
                        logger.info(f"任务 [{task_name}] 第{retry_count}次重试，等待{wait_time}秒...")
                        self._log_scheduler("warning", f"第 {retry_count} 次重试，等待 {wait_time} 秒")
                        time.sleep(wait_time)
                    
                    # 调用整理方法
                    self._log_scheduler("info", f"调用传输链进行文件整理 (尝试 {retry_count + 1}/{self._max_retries + 1})")
                    success, message = self._transfer_chain.manual_transfer(
                        fileitem=fileitem,
                        target_storage=target_storage,
                        target_path=Path(target_path),
                        transfer_type=transfer_type,
                        min_filesize=min_filesize * 1024 * 1024 if min_filesize else 0,  # 转换为字节
                        scrape=scrape,
                        library_category_folder=library_category_folder,
                        library_type_folder=library_type_folder,
                        background=True
                    )
                    
                    if success:
                        messages.append(f"✅ {task_name}: 整理成功")
                        logger.info(f"任务 [{task_name}] 执行成功")
                        self._log_scheduler("info", f"✅ 任务执行成功: {message}")
                        return True
                    else:
                        # 检查是否是认证相关错误
                        if self._is_auth_error(message):
                            if retry_count < self._max_retries:
                                logger.warning(f"任务 [{task_name}] 认证失败，将进行重试: {message}")
                                self._log_scheduler("warning", f"认证失败，准备重试: {message}")
                                continue
                        
                        messages.append(f"❌ {task_name}: {message}")
                        logger.error(f"任务 [{task_name}] 执行失败: {message}")
                        self._log_scheduler("error", f"❌ 任务执行失败: {message}")
                        return False
                        
                except Exception as e:
                    error_msg = str(e)
                    
                    # 检查是否是认证相关异常
                    if self._is_auth_error(error_msg) and retry_count < self._max_retries:
                        logger.warning(f"任务 [{task_name}] 遇到认证异常，将进行重试: {error_msg}")
                        continue
                    
                    if retry_count >= self._max_retries:
                        messages.append(f"❌ {task_name}: {error_msg}")
                        logger.error(f"执行任务 [{task_name}] 重试{self._max_retries}次后仍失败: {error_msg}")
                        return False
                    
            return False
                    
        except Exception as e:
            error_msg = f"执行任务 [{task_name}] 时发生异常: {str(e)}"
            messages.append(f"❌ {task_name}: {str(e)}")
            logger.error(error_msg, exc_info=True)
            self._log_scheduler("error", error_msg)
            return False

    def _check_storage_connection(self, storage_name: str) -> bool:
        """
        检查存储连接状态 - 增强版本
        """
        try:
            self._log_scheduler("debug", f"开始检查存储连接状态: {storage_name}")
            
            # 这里可以添加实际的存储连接检查逻辑
            # 目前简单返回True，实际项目中可以调用存储模块的连接检查方法
            
            # 模拟连接检查延迟
            import time
            time.sleep(0.1)
            
            self._log_scheduler("debug", f"存储连接状态检查完成: {storage_name}")
            return True
            
        except Exception as e:
            error_msg = f"检查存储 {storage_name} 连接状态失败: {str(e)}"
            logger.error(error_msg)
            self._log_scheduler("error", error_msg)
            return False

    def _execute_cleanup_sql(self):
        """
        执行SQL清理操作，删除dest_storage='local'的转移历史记录
        增强版本：添加更多控制和日志记录
        """
        cleanup_start = datetime.now()
        
        try:
            # 获取数据库操作对象
            transferhis = TransferHistoryOper()
            
            # 检查是否有数据库会话
            if not transferhis._db:
                from app.db import ScopedSession
                transferhis._db = ScopedSession()
                should_close = True
            else:
                should_close = False
            
            try:
                # 执行查询操作 - 统计要删除的记录数
                self._log_scheduler("debug", "开始查询需要清理的转移历史记录")
                count_result = transferhis._db.execute(
                    text("SELECT COUNT(*) FROM transferhistory WHERE dest_storage = 'local'")
                ).scalar()
                
                if count_result > 0:
                    self._log_scheduler("info", f"发现 {count_result} 条dest_storage='local'的转移历史记录需要清理")
                    
                    # 执行删除操作
                    self._log_scheduler("debug", "开始执行删除操作")
                    delete_result = transferhis._db.execute(
                        text("DELETE FROM transferhistory WHERE dest_storage = 'local'")
                    )
                    
                    # 提交事务
                    transferhis._db.commit()
                    
                    affected_rows = delete_result.rowcount if hasattr(delete_result, 'rowcount') else count_result
                    self._log_scheduler("info", f"成功删除 {affected_rows} 条dest_storage='local'的转移历史记录")
                    
                    # 记录清理耗时
                    cleanup_time = (datetime.now() - cleanup_start).total_seconds()
                    self._log_scheduler("debug", f"SQL清理操作完成，耗时: {cleanup_time:.2f}秒")
                else:
                    self._log_scheduler("debug", "没有需要清理的转移历史记录")
                
            except Exception as e:
                # 回滚事务
                if transferhis._db:
                    transferhis._db.rollback()
                raise e
            finally:
                # 如果是我们创建的会话，需要关闭
                if should_close and transferhis._db:
                    transferhis._db.close()
                    
        except Exception as e:
            error_msg = f"执行SQL清理操作失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self._log_scheduler("error", error_msg)
            
            # 记录清理失败耗时
            cleanup_time = (datetime.now() - cleanup_start).total_seconds()
            self._log_scheduler("debug", f"SQL清理操作失败，耗时: {cleanup_time:.2f}秒")

    def _should_execute_cleanup(self, task: dict = None) -> bool:
        """
        判断是否应该执行SQL清理操作
        基于任务配置和清理频率控制
        """
        try:
            # 获取当前时间
            now = datetime.now()
            
            # 检查是否有清理频率配置
            cleanup_frequency = task.get('cleanup_frequency', 'always') if task else 'always'
            
            if cleanup_frequency == 'never':
                self._log_scheduler("debug", "跳过SQL清理：任务配置为从不清理")
                return False
            elif cleanup_frequency == 'daily':
                # 检查是否今天已经清理过
                last_cleanup = self._task_status.get('last_cleanup')
                if last_cleanup:
                    last_cleanup_time = datetime.fromisoformat(last_cleanup)
                    if last_cleanup_time.date() == now.date():
                        self._log_scheduler("debug", "跳过SQL清理：今天已经清理过")
                        return False
                # 更新最后清理时间
                self._task_status['last_cleanup'] = now.isoformat()
                return True
            elif cleanup_frequency == 'weekly':
                # 检查是否本周已经清理过
                last_cleanup = self._task_status.get('last_cleanup')
                if last_cleanup:
                    last_cleanup_time = datetime.fromisoformat(last_cleanup)
                    if last_cleanup_time.isocalendar()[1] == now.isocalendar()[1]:
                        self._log_scheduler("debug", "跳过SQL清理：本周已经清理过")
                        return False
                # 更新最后清理时间
                self._task_status['last_cleanup'] = now.isoformat()
                return True
            else:  # 'always' or other values
                return True
                
        except Exception as e:
            self._log_scheduler("warning", f"判断清理频率失败，默认执行清理: {str(e)}")
            return True

    def _is_task_processed(self, task: dict) -> bool:
        """
        检查任务的源路径是否已经成功整理过
        优化检查逻辑，支持更准确的判断
        """
        try:
            source_path = task.get('source_path')
            target_path = task.get('target_path')
            target_storage = task.get('target_storage', 'local')
            
            if not source_path:
                return False
            
            # 查询转移历史记录
            transferhis = TransferHistoryOper()
            
            # 1. 首先检查完全匹配的源路径
            history = transferhis.get_by_src(source_path, storage="local")
            if history and history.status:
                # 检查目标路径是否匹配（避免因为目标路径改变而误判）
                if target_path and target_storage:
                    # 如果目标路径也匹配，则确认已处理
                    if (history.dest and 
                        history.dest.startswith(target_path) and 
                        history.dest_storage == target_storage):
                        return True
                else:
                    # 如果没有指定目标路径，仅凭源路径判断
                    return True
            
            # 2. 检查源路径下的子文件/子目录是否有转移记录
            # 这样可以避免整个目录重复处理
            source_path_obj = Path(source_path)
            if source_path_obj.is_dir():
                # 获取目录下的所有文件和子目录
                try:
                    sub_items = list(source_path_obj.iterdir())
                    if sub_items:
                        # 检查是否有超过50%的子项已经被处理过
                        processed_count = 0
                        for sub_item in sub_items[:10]:  # 只检查前10个，避免性能问题
                            sub_history = transferhis.get_by_src(str(sub_item), storage="local")
                            if sub_history and sub_history.status:
                                processed_count += 1
                        
                        # 如果超过50%的子项已处理，认为整个目录已处理
                        if processed_count > len(sub_items[:10]) * 0.5:
                            return True
                except Exception as e:
                    logger.debug(f"检查子目录时出错: {str(e)}")
            
            return False
            
        except Exception as e:
            logger.error(f"检查任务处理状态失败: {str(e)}")
            return False

    def _is_auth_error(self, error_message: str) -> bool:
        """
        判断是否是认证相关错误
        """
        auth_keywords = [
            "请先扫码登录",
            "refresh frequently",
            "access_token",
            "登录失败",
            "认证失败",
            "token",
            "unauthorized"
        ]
        
        if not error_message:
            return False
            
        error_lower = error_message.lower()
        return any(keyword.lower() in error_lower for keyword in auth_keywords)

    def _execute_cleanup_sql(self):
        """
        执行SQL清理操作，删除dest_storage='local'的转移历史记录
        静默执行，只记录日志，不影响执行流程
        """
        try:
            # 获取数据库操作对象
            transferhis = TransferHistoryOper()
            
            # 检查是否有数据库会话
            if not transferhis._db:
                from app.db import ScopedSession
                transferhis._db = ScopedSession()
                should_close = True
            else:
                should_close = False
            
            try:
                # 执行查询操作 - 统计要删除的记录数
                count_result = transferhis._db.execute(
                    text("SELECT COUNT(*) FROM transferhistory WHERE dest_storage = 'local'")
                ).scalar()
                
                logger.info(f"文件扫描整理：发现 {count_result} 条dest_storage='local'的转移历史记录")
                
                # 执行删除操作
                delete_result = transferhis._db.execute(
                    text("DELETE FROM transferhistory WHERE dest_storage = 'local'")
                )
                
                # 提交事务
                transferhis._db.commit()
                
                affected_rows = delete_result.rowcount if hasattr(delete_result, 'rowcount') else count_result
                logger.info(f"文件扫描整理：成功删除 {affected_rows} 条dest_storage='local'的转移历史记录")
                
            except Exception as e:
                # 回滚事务
                transferhis._db.rollback()
                raise e
            finally:
                # 如果是我们创建的会话，需要关闭
                if should_close:
                    transferhis._db.close()
                    
        except Exception as e:
            error_msg = f"执行SQL清理操作失败: {str(e)}"
            logger.error(error_msg, exc_info=True)

    def _get_task_summary(self, task_index: int = None, task_name: str = None):
        """
        获取任务概要信息
        :param task_index: 任务索引（可选）
        :param task_name: 任务名称（可选）
        :return: 任务概要信息和执行链接
        """
        try:
            # 准备返回的任务列表
            tasks_info = []
            
            # 如果指定了具体任务
            if task_index is not None or task_name:
                target_task = None
                target_index = None
                
                if task_index is not None:
                    if 0 <= task_index < len(self._tasks):
                        target_task = self._tasks[task_index]
                        target_index = task_index
                    else:
                        return {
                            "success": False,
                            "message": f"任务索引 {task_index} 无效，有效范围：0-{len(self._tasks)-1}"
                        }
                elif task_name:
                    for idx, task in enumerate(self._tasks):
                        if task.get('name') == task_name:
                            target_task = task
                            target_index = idx
                            break
                    
                    if not target_task:
                        return {
                            "success": False,
                            "message": f"未找到名称为 '{task_name}' 的任务"
                        }
                
                # 构建单个任务信息
                task_info = {
                    "index": target_index,
                    "name": target_task.get('name', f'任务{target_index+1}'),
                    "enabled": target_task.get('enabled', True),
                    "source_path": target_task.get('source_path', ''),
                    "target_storage": target_task.get('target_storage', 'local'),
                    "target_path": target_task.get('target_path', ''),
                    "transfer_type": target_task.get('transfer_type', 'copy'),
                    "min_filesize": target_task.get('min_filesize', 0),
                    "execute_url": f"/api/v1/plugin/FileScanner/execute_task?task_index={target_index}&action=execute&apikey={{API_KEY}}"
                }
                tasks_info.append(task_info)
                
                return {
                    "success": True,
                    "message": f"任务概要信息: {task_info['name']}",
                    "total_tasks": 1,
                    "enabled_tasks": 1 if task_info['enabled'] else 0,
                    "task": task_info,
                    "execute_url": task_info['execute_url']
                }
            
            # 显示所有任务的概要信息
            enabled_count = 0
            for idx, task in enumerate(self._tasks):
                if not isinstance(task, dict):
                    continue
                    
                enabled = task.get('enabled', True)
                if enabled:
                    enabled_count += 1
                
                task_info = {
                    "index": idx,
                    "name": task.get('name', f'任务{idx+1}'),
                    "enabled": enabled,
                    "source_path": task.get('source_path', ''),
                    "target_storage": task.get('target_storage', 'local'),
                    "target_path": task.get('target_path', ''),
                    "transfer_type": task.get('transfer_type', 'copy'),
                    "min_filesize": task.get('min_filesize', 0),
                    "execute_url": f"/api/v1/plugin/FileScanner/execute_task?task_index={idx}&action=execute&apikey={{API_KEY}}"
                }
                tasks_info.append(task_info)
            
            return {
                "success": True,
                "message": f"共 {len(self._tasks)} 个任务，其中 {enabled_count} 个已启用",
                "total_tasks": len(self._tasks),
                "enabled_tasks": enabled_count,
                "tasks": tasks_info,
                "execute_all_url": "/api/v1/plugin/FileScanner/execute_task?action=execute&apikey={API_KEY}",
                "help": {
                    "usage": "在URL中将 {API_KEY} 替换为您的实际API Key",
                    "execute_all": "访问 execute_all_url 执行所有已启用的任务",
                    "execute_single": "访问任务的 execute_url 执行单个任务"
                }
            }
            
        except Exception as e:
            error_msg = f"获取任务概要时发生异常: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return {
                "success": False,
                "message": error_msg
            }

    def _execute_all_enabled_tasks(self):
        """
        执行所有已启用的任务
        :return: 执行结果
        """
        try:
            if not self._enabled:
                return {
                    "success": False,
                    "message": "插件未启用，无法执行任务"
                }
            
            if not self._tasks:
                return {
                    "success": False,
                    "message": "未配置任何任务"
                }
            
            # 获取所有已启用的任务
            enabled_tasks = [task for task in self._tasks if isinstance(task, dict) and task.get('enabled', True)]
            
            if not enabled_tasks:
                return {
                    "success": False,
                    "message": "没有已启用的任务"
                }
            
            logger.info(f"API触发执行所有任务，共 {len(enabled_tasks)} 个已启用任务")
            
            # 执行SQL清理操作（静默执行，只记录日志）
            self._execute_cleanup_sql()
            
            # 统计信息
            total_tasks = len(enabled_tasks)
            success_tasks = 0
            failed_tasks = 0
            skipped_tasks = 0
            messages = []
            
            # 执行所有已启用的任务
            for idx, task in enumerate(enabled_tasks):
                task_name = task.get('name', f'任务{idx+1}')
                
                # 检查是否跳过已整理文件
                if self._skip_processed and self._is_task_processed(task):
                    skipped_tasks += 1
                    messages.append(f"⏭️ {task_name}: 已整理过，跳过")
                    logger.info(f"任务 [{task_name}] 已整理过，跳过处理")
                    continue
                
                # 执行单个任务
                task_messages = []
                success = self._execute_single_task(task, task_name, task_messages)
                
                if success:
                    success_tasks += 1
                else:
                    failed_tasks += 1
                
                # 收集消息
                if task_messages:
                    messages.extend(task_messages)
            
            # 准备返回结果
            result_message = f"执行完成: 总计 {total_tasks} 个, 成功 {success_tasks} 个, 失败 {failed_tasks} 个, 跳过 {skipped_tasks} 个"
            
            # 发送通知（如果启用）
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="批量执行任务完成",
                    text=result_message + "\n\n" + "\n".join(messages) if messages else result_message
                )
            
            # 判断整体是否成功
            overall_success = success_tasks > 0 or (total_tasks == skipped_tasks and total_tasks > 0)
            
            return {
                "success": overall_success,
                "message": result_message,
                "total": total_tasks,
                "success_count": success_tasks,
                "failed_count": failed_tasks,
                "skipped_count": skipped_tasks,
                "details": messages
            }
            
        except Exception as e:
            error_msg = f"批量执行任务时发生异常: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return {
                "success": False,
                "message": error_msg
            }

    def downloader_tasks_api(self, status: str = "all", page: int = 1, count: int = 20):
        """
        获取下载任务列表（使用MoviePilot的TorrentStatus枚举）
        :param status: 任务状态 - "all"(全部), "downloading"(下载中), "completed"(已完成)
        :param page: 页码
        :param count: 每页数量
        :return: 下载任务列表
        """
        try:
            from app.chain.download import DownloadChain
            from app.schemas.types import TorrentStatus
            from datetime import datetime
            
            download_chain = DownloadChain()
            all_tasks = []
            
            # 获取下载中的任务
            if status in ["all", "downloading"]:
                try:
                    downloading_tasks = download_chain.downloading()
                    if downloading_tasks:
                        for task in downloading_tasks:
                            task_dict = task.dict()
                            task_dict["status"] = "downloading"
                            task_dict["status_text"] = "下载中"
                            # 添加时间戳用于排序（下载中的任务使用当前时间）
                            task_dict["sort_time"] = datetime.now().timestamp()
                            all_tasks.append(task_dict)
                            
                except Exception as e:
                    logger.warning(f"获取下载中任务失败: {str(e)}")
            
            # 获取已完成/可转移的任务
            if status in ["all", "completed"]:
                try:
                    completed_tasks = download_chain.list_torrents(status=TorrentStatus.TRANSFER)
                    if completed_tasks:
                        for task in completed_tasks:
                            task_dict = task.dict()
                            task_dict["status"] = "completed"
                            task_dict["status_text"] = "已完成"
                            task_dict["progress"] = 100.0  # 已完成的任务进度为100%
                            task_dict["dlspeed"] = "0 B/s"
                            task_dict["left_time"] = "已完成"
                            # 添加时间戳用于排序（使用任务的完成时间或当前时间）
                            task_dict["sort_time"] = getattr(task, 'completion_on', datetime.now().timestamp())
                            all_tasks.append(task_dict)
                            
                except Exception as e:
                    logger.warning(f"获取已完成任务失败: {str(e)}")
            
            # 按时间倒序排序（最新的在前面）
            all_tasks.sort(key=lambda x: x.get("sort_time", 0), reverse=True)
            
            # 计算分页
            total_count = len(all_tasks)
            start_idx = (page - 1) * count
            end_idx = start_idx + count
            result_tasks = all_tasks[start_idx:end_idx]
            
            # 移除排序用的时间戳字段
            for task in result_tasks:
                task.pop("sort_time", None)
            
            # 计算状态统计
            downloading_count = len([t for t in all_tasks if t.get("status") == "downloading"])
            completed_count = len([t for t in all_tasks if t.get("status") == "completed"])
            
            return {
                "success": True,
                "tasks": result_tasks,
                "count": len(result_tasks),
                "total": total_count,
                "page": page,
                "page_count": count,
                "total_pages": (total_count + count - 1) // count,  # 计算总页数
                "status_filter": status,
                "status_summary": {
                    "downloading": downloading_count,
                    "completed": completed_count,
                    "total": total_count
                }
            }
            
        except Exception as e:
            logger.error(f"获取下载任务失败: {str(e)}", exc_info=True)
            return {
                "success": False,
                "message": str(e),
                "tasks": [],
                "count": 0,
                "total": 0,
                "page": page,
                "page_count": count,
                "total_pages": 0,
                "status_filter": status,
                "status_summary": {
                    "downloading": 0,
                    "completed": 0,
                    "total": 0
                }
            }

    def dashboard_api(self):
        """
        FileScanner 控制面板 - 返回用户友好的 HTML 界面
        :return: HTML 页面内容
        """
        try:
            # 获取任务统计信息
            total_tasks = len(self._tasks) if self._tasks else 0
            enabled_tasks = len([task for task in (self._tasks or []) if isinstance(task, dict) and task.get('enabled', True)])
            
            # 生成任务卡片 HTML
            task_cards_html = ""
            if self._tasks:
                for idx, task in enumerate(self._tasks):
                    if not isinstance(task, dict):
                        continue
                    
                    enabled = task.get('enabled', True)
                    task_name = task.get('name', f'任务{idx+1}')
                    source_path = task.get('source_path', '未设置')
                    target_storage = task.get('target_storage', 'local')
                    target_path = task.get('target_path', '未设置')
                    transfer_type = task.get('transfer_type', 'copy')
                    min_filesize = task.get('min_filesize', 0)
                    
                    status_class = "enabled" if enabled else "disabled"
                    status_text = "✅ 已启用" if enabled else "❌ 已禁用"
                    button_disabled = "" if enabled else "disabled"
                    
                    task_cards_html += f'''
                    <div class="task-card {status_class}">
                        <div class="task-header">
                            <h3>{task_name}</h3>
                            <span class="status {status_class}">{status_text}</span>
                        </div>
                        <div class="task-details">
                            <p><strong>📂 源路径：</strong>{source_path}</p>
                            <p><strong>📁 目标：</strong>{target_storage}:{target_path}</p>
                            <p><strong>🔄 方式：</strong>{transfer_type}</p>
                            <p><strong>📏 最小大小：</strong>{min_filesize} MB</p>
                            <p><strong>⏰ 定时执行：</strong>{task.get('cron', '0 3 * * *')}</p>
                        </div>
                        <div class="task-actions">
                            <button onclick="executeTask({idx})" {button_disabled} class="execute-btn">
                                ⚡ 立即执行
                            </button>
                            <span class="task-index">索引: {idx}</span>
                        </div>
                    </div>
                    '''
            else:
                task_cards_html = '<div class="no-tasks">📋 暂无配置任务</div>'
            
            # 生成完整的 HTML 页面
            html_content = f'''
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>📁 FileScanner 控制面板</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}
        
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: rgba(255, 255, 255, 0.95);
            border-radius: 20px;
            padding: 30px;
            box-shadow: 0 20px 40px rgba(0,0,0,0.1);
        }}
        
        h1 {{
            text-align: center;
            margin-bottom: 30px;
            color: #333;
            font-size: 2.5em;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
        }}
        
        .summary {{
            background: #f8f9ff;
            padding: 20px;
            border-radius: 15px;
            margin-bottom: 30px;
            text-align: center;
            border-left: 5px solid #667eea;
        }}
        
        .summary p {{
            font-size: 1.2em;
            color: #555;
            margin: 10px 0;
        }}
        
        .global-controls {{
            text-align: center;
            margin-bottom: 30px;
        }}
        
        .global-controls button {{
            background: linear-gradient(45deg, #667eea, #764ba2);
            color: white;
            border: none;
            padding: 15px 30px;
            margin: 0 10px;
            border-radius: 25px;
            font-size: 1.1em;
            cursor: pointer;
            transition: all 0.3s ease;
            box-shadow: 0 5px 15px rgba(0,0,0,0.2);
        }}
        
        .global-controls button:hover {{
            transform: translateY(-2px);
            box-shadow: 0 8px 25px rgba(0,0,0,0.3);
        }}
        
        .global-controls button:active {{
            transform: translateY(0);
        }}
        
        .tasks-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 25px;
        }}
        
        .task-card {{
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            border: 2px solid #e0e0e0;
            transition: all 0.3s ease;
        }}
        
        .task-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 15px 40px rgba(0,0,0,0.2);
        }}
        
        .task-card.enabled {{
            border-left: 5px solid #4CAF50;
        }}
        
        .task-card.disabled {{
            border-left: 5px solid #f44336;
            opacity: 0.7;
        }}
        
        .task-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            border-bottom: 1px solid #eee;
            padding-bottom: 15px;
        }}
        
        .task-header h3 {{
            color: #333;
            font-size: 1.3em;
        }}
        
        .status {{
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 0.9em;
            font-weight: bold;
        }}
        
        .status.enabled {{
            background: #e8f5e8;
            color: #4CAF50;
        }}
        
        .status.disabled {{
            background: #ffebee;
            color: #f44336;
        }}
        
        .task-details p {{
            margin: 10px 0;
            color: #666;
            line-height: 1.5;
        }}
        
        .task-actions {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-top: 20px;
            padding-top: 15px;
            border-top: 1px solid #eee;
        }}
        
        .execute-btn {{
            background: linear-gradient(45deg, #4CAF50, #45a049);
            color: white;
            border: none;
            padding: 12px 25px;
            border-radius: 20px;
            cursor: pointer;
            font-size: 1em;
            transition: all 0.3s ease;
            box-shadow: 0 3px 10px rgba(76, 175, 80, 0.3);
        }}
        
        .execute-btn:hover:not(:disabled) {{
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(76, 175, 80, 0.4);
        }}
        
        .execute-btn:disabled {{
            background: #ccc;
            cursor: not-allowed;
            box-shadow: none;
        }}
        
        .task-index {{
            color: #888;
            font-size: 0.9em;
        }}
        
        .no-tasks {{
            text-align: center;
            padding: 50px;
            color: #999;
            font-size: 1.2em;
        }}
        
        .result-message {{
            position: fixed;
            top: 20px;
            right: 20px;
            padding: 15px 25px;
            border-radius: 10px;
            color: white;
            font-weight: bold;
            z-index: 1000;
            opacity: 0;
            transform: translateX(100%);
            transition: all 0.3s ease;
        }}
        
        .result-message.success {{
            background: #4CAF50;
        }}
        
        .result-message.error {{
            background: #f44336;
        }}
        
        .result-message.show {{
            opacity: 1;
            transform: translateX(0);
        }}
        
        .api-key-input {{
            background: #fff3cd;
            border: 1px solid #ffeaa7;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 20px;
            display: none;
        }}
        
        .api-key-input input {{
            width: 100%;
            padding: 10px;
            border: 1px solid #ddd;
            border-radius: 5px;
            margin: 10px 0;
        }}
        
        .api-key-input button {{
            background: #007bff;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 5px;
            cursor: pointer;
        }}
        
        /* Tab 样式 */
        .tabs {{
            display: flex;
            border-bottom: 2px solid #667eea;
            margin-bottom: 30px;
            background: white;
            border-radius: 10px 10px 0 0;
            overflow: hidden;
        }}
        
        .tab {{
            flex: 1;
            padding: 15px 30px;
            cursor: pointer;
            text-align: center;
            background: #f5f5f5;
            transition: all 0.3s ease;
            font-weight: 500;
            font-size: 1.1em;
            border-right: 1px solid #e0e0e0;
        }}
        
        .tab:last-child {{
            border-right: none;
        }}
        
        .tab:hover {{
            background: #e8e8ff;
        }}
        
        .tab.active {{
            background: #667eea;
            color: white;
        }}
        
        .tab-content {{
            display: none;
        }}
        
        .tab-content.active {{
            display: block;
        }}
        
        /* 下载任务卡片样式 */
        .download-card {{
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            border: 2px solid #e0e0e0;
            transition: all 0.3s ease;
            margin-bottom: 20px;
        }}
        
        .download-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 15px 40px rgba(0,0,0,0.2);
        }}
        
        .download-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
        }}
        
        .download-title {{
            font-size: 1.2em;
            font-weight: bold;
            color: #333;
            max-width: 70%;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}
        
        .download-status {{
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 0.9em;
            font-weight: bold;
            background: #e3f2fd;
            color: #1976d2;
        }}
        
        .progress-bar {{
            width: 100%;
            height: 20px;
            background: #f0f0f0;
            border-radius: 10px;
            overflow: hidden;
            margin: 15px 0;
        }}
        
        .progress-fill {{
            height: 100%;
            background: linear-gradient(45deg, #4CAF50, #45a049);
            transition: width 0.3s ease;
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-size: 0.8em;
            font-weight: bold;
        }}
        
        .download-stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
            margin-top: 15px;
        }}
        
        .stat-item {{
            text-align: center;
            padding: 10px;
            background: #f8f9ff;
            border-radius: 8px;
        }}
        
        .stat-label {{
            font-size: 0.8em;
            color: #666;
            margin-bottom: 5px;
        }}
        
        .stat-value {{
            font-size: 1.1em;
            font-weight: bold;
            color: #333;
        }}
        
        .no-downloads {{
            text-align: center;
            padding: 80px 20px;
            color: #999;
            font-size: 1.2em;
        }}
        
        .refresh-info {{
            text-align: center;
            color: #666;
            font-size: 0.9em;
            margin-top: 20px;
        }}
        
        @media (max-width: 768px) {{
            .container {{
                padding: 15px;
            }}
            
            .tasks-grid {{
                grid-template-columns: 1fr;
            }}
            
            .global-controls button {{
                display: block;
                width: 100%;
                margin: 10px 0;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📁 FileScanner 控制面板</h1>
        
        <div class="summary">
            <p><strong>插件状态：</strong>{"🟢 已启用" if self._enabled else "🔴 已禁用"}</p>
            <p><strong>总任务数：</strong>{total_tasks} 个 | <strong>已启用：</strong>{enabled_tasks} 个</p>
            <p><strong>跳过已整理：</strong>{"✅ 开启" if self._skip_processed else "❌ 关闭"}</p>
        </div>
        
        <div class="api-key-input" id="apiKeyInput">
            <p><strong>⚠️ 需要输入 API Key 才能执行任务：</strong></p>
            <input type="text" id="apiKeyField" placeholder="请输入您的 API Key" />
            <button onclick="saveApiKey()">保存</button>
        </div>
        
        <!-- Tab 导航 -->
        <div class="tabs">
            <div class="tab active" onclick="switchTab('scanner')">📂 扫描任务</div>
            <div class="tab" onclick="switchTab('downloader')">📥 下载监控</div>
        </div>
        
        <!-- Tab 内容区域 -->
        <div id="scanner-tab" class="tab-content active">
            <div class="global-controls">
                <button onclick="refreshPage()">🔄 刷新页面</button>
                <button onclick="showApiKeyInput()">🔑 设置 API Key</button>
            </div>
            
            <div class="tasks-grid">
                {task_cards_html}
            </div>
        </div>
        
        <div id="downloader-tab" class="tab-content">
            <!-- 下载任务过滤器 -->
            <div class="download-filters" style="margin-bottom: 20px;">
                <div style="display: flex; justify-content: space-between; align-items: center; background: white; padding: 20px; border-radius: 15px; box-shadow: 0 5px 15px rgba(0,0,0,0.1);">
                    <div style="display: flex; gap: 15px; align-items: center;">
                        <label style="font-weight: bold; color: #333;">状态过滤：</label>
                        <select id="statusFilter" onchange="filterTasks()" style="padding: 8px 15px; border: 1px solid #ddd; border-radius: 8px; background: white;">
                            <option value="all">全部任务</option>
                            <option value="downloading">下载中</option>
                            <option value="completed">已完成</option>
                        </select>
                    </div>
                    <div id="download-summary" style="font-size: 0.9em; color: #666;">
                        <span id="summary-text">正在加载...</span>
                    </div>
                </div>
            </div>
            
            <div id="download-tasks-container">
                <div class="no-downloads">
                    <p>📡 正在加载下载任务...</p>
                </div>
            </div>
            
            <!-- 分页控制 -->
            <div id="pagination-container" style="text-align: center; margin-top: 20px; display: none;">
                <div style="display: inline-flex; gap: 10px; align-items: center; background: white; padding: 15px; border-radius: 15px; box-shadow: 0 5px 15px rgba(0,0,0,0.1);">
                    <button id="prevPage" onclick="changePage(-1)" style="padding: 8px 15px; border: 1px solid #ddd; border-radius: 8px; background: white; cursor: pointer;">上一页</button>
                    <span id="pageInfo" style="margin: 0 15px; color: #666;">第 1 页</span>
                    <button id="nextPage" onclick="changePage(1)" style="padding: 8px 15px; border: 1px solid #ddd; border-radius: 8px; background: white; cursor: pointer;">下一页</button>
                </div>
            </div>
            
            <div class="refresh-info">
                <p>🔄 每10秒自动刷新</p>
            </div>
        </div>
    </div>
    
    <div id="resultMessage" class="result-message"></div>
    
    <script>
        // 获取 API Key - 增强Safari兼容性
        function getApiKey() {{
            try {{
                // 保持原有逻辑，添加Safari特殊处理
                return localStorage.getItem('filescanner_api_key') || 
                       localStorage.getItem('api_token') || 
                       sessionStorage.getItem('api_token') ||
                       getSafariApiKey();
            }} catch (e) {{
                window.FileScanner.log('Storage access failed, trying Safari method:', e.message);
                return getSafariApiKey();
            }}
        }}
        
        // Safari专用API Key获取方法
        function getSafariApiKey() {{
            try {{
                // Safari可能需要特殊的存储访问方式
                if (window.safari) {{
                    // iOS Safari特殊处理
                    const safariKey = document.cookie
                        .split('; ')
                        .find(row => row.startsWith('api_token='))
                        ?.split('=')[1];
                    if (safariKey) return safariKey;
                }}
                
                // 尝试从URL参数获取（手动输入的情况）
                const urlParams = new URLSearchParams(window.location.search);
                const urlKey = urlParams.get('apikey');
                if (urlKey) {{
                    // 临时保存到内存
                    window._tempApiKey = urlKey;
                    return urlKey;
                }}
                
                // 返回临时保存的key
                return window._tempApiKey || null;
            }} catch (e) {{
                window.FileScanner.log('Safari API key fallback failed:', e.message);
                return null;
            }}
        }}
        
        // 保存 API Key
        function saveApiKey() {{
            const apiKey = document.getElementById('apiKeyField').value.trim();
            if (apiKey) {{
                localStorage.setItem('filescanner_api_key', apiKey);
                document.getElementById('apiKeyInput').style.display = 'none';
                showResult(true, 'API Key 已保存');
            }} else {{
                showResult(false, '请输入有效的 API Key');
            }}
        }}
        
        // 显示 API Key 输入框
        function showApiKeyInput() {{
            document.getElementById('apiKeyInput').style.display = 'block';
            document.getElementById('apiKeyField').value = getApiKey() || '';
        }}
        
        // Safari浏览器检测
        function isSafari() {{
            return /^((?!chrome|android).)*safari/i.test(navigator.userAgent) || 
                   !!window.safari ||
                   /iPad|iPhone|iPod/.test(navigator.userAgent);
        }}
        
        // 初始化渐进式事件处理
        function initEventHandlers() {{
            // 只在Safari或onclick不可用时替换事件处理
            if (isSafari() || !supportsOnclick()) {{
                window.FileScanner.log('使用现代事件处理机制 (Safari兼容)');
                replaceOnclickHandlers();
            }} else {{
                window.FileScanner.log('使用标准onclick事件处理');
            }}
        }}
        
        // 检测onclick支持
        function supportsOnclick() {{
            try {{
                const testBtn = document.createElement('button');
                testBtn.onclick = function() {{}};
                return typeof testBtn.onclick === 'function';
            }} catch (e) {{
                return false;
            }}
        }}
        
        // 替换onclick为addEventListener（仅Safari需要时使用）
        function replaceOnclickHandlers() {{
            // 替换任务执行按钮
            document.querySelectorAll('[onclick*="executeTask"]').forEach(btn => {{
                const match = btn.getAttribute('onclick').match(/executeTask\\((\\d+)\\)/);
                if (match) {{
                    const taskIndex = parseInt(match[1]);
                    btn.removeAttribute('onclick');
                    btn.addEventListener('click', function(e) {{
                        e.preventDefault();
                        executeTask(taskIndex);
                    }});
                    window.FileScanner.log(`Replaced onclick for task ${{taskIndex}}`);
                }}
            }});
            
            // 替换其他按钮事件
            const eventMap = {{
                'refreshPage()': refreshPage,
                'showApiKeyInput()': showApiKeyInput,
                'saveApiKey()': saveApiKey,
                'executeAll()': executeAll
            }};
            
            Object.keys(eventMap).forEach(funcCall => {{
                document.querySelectorAll(`[onclick="${{funcCall}}"]`).forEach(btn => {{
                    btn.removeAttribute('onclick');
                    btn.addEventListener('click', function(e) {{
                        e.preventDefault();
                        eventMap[funcCall]();
                    }});
                    window.FileScanner.log(`Replaced onclick for ${{funcCall}}`);
                }});
            }});
        }}
        
        // Safari兼容的网络请求包装函数
        async function safeFetch(url, options = {{}}) {{
            const defaultOptions = {{
                method: 'GET',
                cache: 'no-cache',
                ...options
            }};
            
            // Safari特殊配置
            if (isSafari()) {{
                defaultOptions.credentials = 'same-origin';
                defaultOptions.mode = 'cors';
                // Safari可能需要明确的headers
                defaultOptions.headers = {{
                    'Accept': 'application/json',
                    'Content-Type': 'application/json',
                    ...defaultOptions.headers
                }};
                window.FileScanner.log('使用Safari兼容配置');
            }}
            
            try {{
                window.FileScanner.log('发送请求到:', url, defaultOptions);
                const response = await fetch(url, defaultOptions);
                
                window.FileScanner.log('响应状态:', {{
                    status: response.status,
                    statusText: response.statusText,
                    ok: response.ok
                }});
                
                return response;
            }} catch (error) {{
                window.FileScanner.log('网络请求失败:', error);
                // Safari fallback: 尝试简化请求
                if (isSafari() && options.retry !== false) {{
                    window.FileScanner.log('尝试Safari fallback请求');
                    return safeFetch(url, {{ ...options, retry: false, mode: 'no-cors' }});
                }}
                throw error;
            }}
        }}
        
        // 执行单个任务
        async function executeTask(taskIndex) {{
            window.FileScanner.log(`开始执行任务 ${{taskIndex}}`);
            const apiKey = getApiKey();
            if (!apiKey) {{
                window.FileScanner.log('API Key 未设置');
                showApiKeyInput();
                showResult(false, '请先设置 API Key');
                return;
            }}
            
            const button = event.target;
            const originalText = button.textContent;
            button.textContent = '⏳ 执行中...';
            button.disabled = true;
            
            try {{
                const url = `/api/v1/plugin/FileScanner/execute_task?task_index=${{taskIndex}}&action=execute&apikey=${{apiKey}}`;
                const response = await safeFetch(url);
                
                if (!response.ok) {{
                    showResult(false, `任务 ${{taskIndex}} 请求失败：HTTP ${{response.status}}`);
                    return;
                }}
                
                let data;
                try {{
                    data = await response.json();
                    window.FileScanner.log('响应数据:', data);
                }} catch (jsonError) {{
                    window.FileScanner.log('JSON 解析错误:', jsonError);
                    showResult(false, `任务 ${{taskIndex}} 响应格式错误：无法解析JSON`);
                    return;
                }}
                
                if (data && data.success) {{
                    const message = data.message || '执行成功';
                    showResult(true, `任务 ${{taskIndex}} 执行成功：${{message}}`);
                }} else {{
                    const message = (data && data.message) || '未知错误';
                    showResult(false, `任务 ${{taskIndex}} 执行失败：${{message}}`);
                }}
            }} catch (error) {{
                console.error('执行任务出错:', error);
                window.FileScanner.log('执行任务错误:', {{
                    error: error.message,
                    stack: error.stack
                }});
                showResult(false, `任务 ${{taskIndex}} 执行错误：${{error.message || '网络请求失败'}}`);
            }} finally {{
                button.textContent = originalText;
                button.disabled = false;
            }}
        }}
        
        // 执行所有任务
        async function executeAll() {{
            const apiKey = getApiKey();
            if (!apiKey) {{
                showApiKeyInput();
                showResult(false, '请先设置 API Key');
                return;
            }}
            
            try {{
                const url = `/api/v1/plugin/FileScanner/execute_task?action=execute&apikey=${{apiKey}}`;
                const response = await safeFetch(url);
                
                if (!response.ok) {{
                    showResult(false, `批量执行请求失败：HTTP ${{response.status}}`);
                    return;
                }}
                
                const data = await response.json();
                
                if (data && data.success) {{
                    const message = data.message || '批量执行成功';
                    showResult(true, `批量执行完成：${{message}}`);
                }} else {{
                    const message = (data && data.message) || '未知错误';
                    showResult(false, `批量执行失败：${{message}}`);
                }}
            }} catch (error) {{
                console.error('批量执行任务出错:', error);
                showResult(false, `批量执行错误：${{error.message || '网络请求失败'}}`);
            }}
        }}
        
        // 显示执行结果
        function showResult(success, message) {{
            const resultDiv = document.getElementById('resultMessage');
            resultDiv.textContent = message;
            resultDiv.className = `result-message ${{success ? 'success' : 'error'}} show`;
            
            setTimeout(() => {{
                resultDiv.classList.remove('show');
            }}, 5000);
        }}
        
        // 刷新页面
        function refreshPage() {{
            window.location.reload();
        }}
        
        // Tab 切换功能
        function switchTab(tabName) {{
            // 更新 Tab 样式
            const tabs = document.querySelectorAll('.tab');
            tabs.forEach(tab => tab.classList.remove('active'));
            event.target.classList.add('active');
            
            // 更新内容显示
            const contents = document.querySelectorAll('.tab-content');
            contents.forEach(content => content.classList.remove('active'));
            document.getElementById(`${{tabName}}-tab`).classList.add('active');
            
            // 如果切换到下载监控，立即刷新数据
            if (tabName === 'downloader') {{
                fetchDownloaderTasks(currentStatus, currentPage);
            }}
        }}
        
        // 全局变量跟踪当前状态和分页
        let currentStatus = 'all';
        let currentPage = 1;
        let totalPages = 1;
        
        // 获取下载任务
        async function fetchDownloaderTasks(status = 'all', page = 1) {{
            const apiKey = getApiKey();
            if (!apiKey) {{
                document.getElementById('download-tasks-container').innerHTML = `
                    <div class="no-downloads">
                        <p>⚠️ 请先设置 API Key 才能查看下载任务</p>
                    </div>
                `;
                return;
            }}
            
            try {{
                const url = `/api/v1/plugin/FileScanner/downloader_tasks?status=${{status}}&page=${{page}}&count=20&apikey=${{apiKey}}`;
                const response = await safeFetch(url);
                
                if (!response.ok) {{
                    document.getElementById('download-tasks-container').innerHTML = `
                        <div class="no-downloads">
                            <p>❌ 获取下载任务失败：HTTP ${{response.status}}</p>
                        </div>
                    `;
                    return;
                }}
                
                const data = await response.json();
                
                if (data && data.success) {{
                    updateDownloaderDisplay(data);
                    updateSummary(data.status_summary || {{}});
                    updatePagination(data);
                }} else {{
                    const message = (data && data.message) || '未知错误';
                    document.getElementById('download-tasks-container').innerHTML = `
                        <div class="no-downloads">
                            <p>❌ 获取下载任务失败：${{message}}</p>
                        </div>
                    `;
                }}
            }} catch (error) {{
                console.error('获取下载任务出错:', error);
                document.getElementById('download-tasks-container').innerHTML = `
                    <div class="no-downloads">
                        <p>❌ 网络错误：${{error.message || '请求失败'}}</p>
                    </div>
                `;
            }}
        }}
        
        // 状态过滤
        function filterTasks() {{
            const statusFilter = document.getElementById('statusFilter');
            currentStatus = statusFilter.value;
            currentPage = 1; // 重置到第一页
            fetchDownloaderTasks(currentStatus, currentPage);
        }}
        
        // 分页切换
        function changePage(direction) {{
            const newPage = currentPage + direction;
            if (newPage >= 1 && newPage <= totalPages) {{
                currentPage = newPage;
                fetchDownloaderTasks(currentStatus, currentPage);
            }}
        }}
        
        // 更新状态摘要
        function updateSummary(summary) {{
            const summaryText = document.getElementById('summary-text');
            if (summary && typeof summary === 'object') {{
                summaryText.textContent = `总计: ${{summary.total || 0}} | 下载中: ${{summary.downloading || 0}} | 已完成: ${{summary.completed || 0}}`;
            }} else {{
                summaryText.textContent = '正在加载...';
            }}
        }}
        
        // 更新分页信息
        function updatePagination(data) {{
            const paginationContainer = document.getElementById('pagination-container');
            const pageInfo = document.getElementById('pageInfo');
            const prevButton = document.getElementById('prevPage');
            const nextButton = document.getElementById('nextPage');
            
            if (data.total > data.page_count) {{ // 只有超过每页数量才显示分页
                paginationContainer.style.display = 'block';
                totalPages = data.total_pages || Math.ceil(data.total / data.page_count);
                pageInfo.textContent = `第 ${{data.page}} 页，共 ${{totalPages}} 页 (${{data.total}} 条记录)`;
                
                prevButton.disabled = data.page <= 1;
                nextButton.disabled = data.page >= totalPages;
                
                currentPage = data.page;
            }} else {{
                paginationContainer.style.display = 'none';
                totalPages = 1;
                currentPage = 1;
            }}
        }}
        
        // 更新下载任务显示
        function updateDownloaderDisplay(data) {{
            const container = document.getElementById('download-tasks-container');
            const tasks = data.tasks || [];
            
            if (!tasks || tasks.length === 0) {{
                const statusText = currentStatus === 'downloading' ? '下载中' : 
                                 currentStatus === 'completed' ? '已完成' : '';
                container.innerHTML = `
                    <div class="no-downloads">
                        <p>📭 当前没有${{statusText}}下载任务</p>
                    </div>
                `;
                return;
            }}
            
            let html = '';
            tasks.forEach(task => {{
                const progress = task.progress || 0;
                const title = task.title || task.name || '未知任务';
                const dlspeed = task.dlspeed || '0 B/s';
                const upspeed = task.upspeed || '0 B/s';
                const size = formatSize(task.size || 0);
                const leftTime = task.left_time || '计算中...';
                const status = task.status || 'downloading';
                const statusText = task.status_text || '未知状态';
                const downloader = task.downloader || '未知';
                
                // 根据状态设置不同的样式
                const statusColor = status === 'downloading' ? '#1976d2' : 
                                  status === 'completed' ? '#4CAF50' : '#666';
                const progressColor = status === 'completed' ? '#4CAF50' : '#2196F3';
                
                html += `
                    <div class="download-card" style="border-left: 4px solid ${{statusColor}};">
                        <div class="download-header">
                            <div class="download-title" title="${{title}}">${{title}}</div>
                            <div class="download-status" style="background: ${{statusColor}}20; color: ${{statusColor}};">
                                ${{statusText}}
                            </div>
                        </div>
                        
                        <div class="progress-bar">
                            <div class="progress-fill" style="width: ${{progress}}%; background: linear-gradient(45deg, ${{progressColor}}, ${{progressColor}}dd);">
                                ${{progress.toFixed(1)}}%
                            </div>
                        </div>
                        
                        <div class="download-stats">
                            <div class="stat-item">
                                <div class="stat-label">下载器</div>
                                <div class="stat-value">${{downloader}}</div>
                            </div>
                            ${{status === 'downloading' ? `
                            <div class="stat-item">
                                <div class="stat-label">下载速度</div>
                                <div class="stat-value">${{dlspeed}}</div>
                            </div>
                            <div class="stat-item">
                                <div class="stat-label">上传速度</div>
                                <div class="stat-value">${{upspeed}}</div>
                            </div>
                            <div class="stat-item">
                                <div class="stat-label">剩余时间</div>
                                <div class="stat-value">${{leftTime}}</div>
                            </div>
                            ` : `
                            <div class="stat-item">
                                <div class="stat-label">文件大小</div>
                                <div class="stat-value">${{formatSize(task.size || 0)}}</div>
                            </div>
                            <div class="stat-item">
                                <div class="stat-label">上传速度</div>
                                <div class="stat-value">${{upspeed}}</div>
                            </div>
                            <div class="stat-item">
                                <div class="stat-label">做种状态</div>
                                <div class="stat-value">已完成</div>
                            </div>
                            `}}
                        </div>
                        
                        ${{task.title && task.title !== task.name ? `
                        <div style="margin-top: 15px; padding-top: 15px; border-top: 1px solid #eee;">
                            <small style="color: #666;">
                                📄 种子：${{task.name || '未知'}}
                            </small>
                        </div>
                        ` : ''}}
                        
                        ${{task.path && status === 'completed' ? `
                        <div style="margin-top: 10px; padding-top: 10px; border-top: 1px solid #eee;">
                            <small style="color: #666;">
                                📁 路径：${{task.path}}
                            </small>
                        </div>
                        ` : ''}}
                        
                        ${{task.hash ? `
                        <div style="margin-top: 10px; padding-top: 10px; border-top: 1px solid #eee;">
                            <small style="color: #888; font-family: monospace; font-size: 0.8em;">
                                🔗 Hash: ${{task.hash.substring(0, 16)}}...
                            </small>
                        </div>
                        ` : ''}}
                    </div>
                `;
            }});
            
            container.innerHTML = html;
        }}
        
        // 格式化文件大小
        function formatSize(bytes) {{
            if (bytes === 0) return '0 B';
            const k = 1024;
            const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        }}
        
        // 定时刷新下载任务
        let refreshInterval;
        function startDownloadRefresh() {{
            // 清除旧的定时器
            if (refreshInterval) {{
                clearInterval(refreshInterval);
            }}
            
            // 每10秒刷新一次
            refreshInterval = setInterval(() => {{
                // 只有在下载监控Tab激活时才刷新
                if (document.getElementById('downloader-tab').classList.contains('active')) {{
                    fetchDownloaderTasks(currentStatus, currentPage);
                }}
            }}, 10000);
        }}
        
        // 添加调试日志功能
        window.FileScanner = {{
            debug: true,
            log: function(message, data) {{
                if (this.debug) {{
                    console.log(`[FileScanner] ${{message}}`, data || '');
                }}
            }}
        }};
        
        // 页面加载时检查 API Key
        document.addEventListener('DOMContentLoaded', function() {{
            window.FileScanner.log('Dashboard 页面加载完成');
            
            // Safari兼容性检测和初始化
            if (isSafari()) {{
                window.FileScanner.log('检测到Safari浏览器，启用兼容模式');
                showResult(true, '🦋 Safari兼容模式已启用');
            }}
            
            // 初始化事件处理（Safari兼容）
            initEventHandlers();
            
            const apiKey = getApiKey();
            if (!apiKey) {{
                window.FileScanner.log('未找到 API Key');
                if (isSafari()) {{
                    // Safari用户特殊提示
                    setTimeout(() => {{
                        showResult(false, '🔑 Safari用户：请手动设置 API Key 或在URL中添加 ?apikey=YOUR_KEY');
                    }}, 1500);
                }} else {{
                    setTimeout(() => {{
                        showResult(false, '提示：请设置 API Key 后才能执行任务');
                    }}, 1000);
                }}
            }} else {{
                window.FileScanner.log('API Key 检测成功');
                if (isSafari()) {{
                    showResult(true, '✅ Safari: API Key 已就绪');
                }}
            }}
            
            // 启动下载任务定时刷新
            startDownloadRefresh();
        }});
    </script>
</body>
</html>
            '''
            
            return Response(content=html_content, media_type="text/html; charset=utf-8")
            
        except Exception as e:
            error_msg = f"生成控制面板时发生异常: {str(e)}"
            logger.error(error_msg, exc_info=True)
            error_html = f'''
<!DOCTYPE html>
<html>
<head><title>错误</title></head>
<body>
    <h1>控制面板加载失败</h1>
    <p>{error_msg}</p>
    <a href="javascript:history.back()">返回</a>
</body>
</html>
            '''
            return Response(content=error_html, media_type="text/html; charset=utf-8")

    def execute_task_api(self, task_index: int = None, task_name: str = None, action: str = None):
        """
        API接口：立即执行指定任务或显示任务概要
        :param task_index: 任务索引（从0开始）
        :param task_name: 任务名称
        :param action: 操作类型，execute=执行任务，其他=显示概要
        :return: 执行结果或任务概要
        """
        try:
            if not self._enabled:
                return {
                    "success": False,
                    "message": "插件未启用"
                }
            
            if not self._tasks:
                return {
                    "success": False,
                    "message": "未配置任何任务"
                }
            
            # 如果不是执行操作，返回任务概要信息
            if action != "execute":
                return self._get_task_summary(task_index, task_name)
            
            # 根据参数找到要执行的任务
            target_task = None
            target_task_name = None
            
            if task_index is not None:
                # 通过索引查找任务
                if 0 <= task_index < len(self._tasks):
                    target_task = self._tasks[task_index]
                    target_task_name = target_task.get('name', f'任务{task_index+1}')
                else:
                    return {
                        "success": False,
                        "message": f"任务索引 {task_index} 无效，有效范围：0-{len(self._tasks)-1}"
                    }
            elif task_name:
                # 通过名称查找任务
                for idx, task in enumerate(self._tasks):
                    if task.get('name') == task_name:
                        target_task = task
                        target_task_name = task_name
                        break
                
                if not target_task:
                    return {
                        "success": False,
                        "message": f"未找到名称为 '{task_name}' 的任务"
                    }
            else:
                # 如果既没有 task_index 也没有 task_name，执行所有已启用的任务
                return self._execute_all_enabled_tasks()
            
            # 检查任务是否启用
            if not target_task.get('enabled', True):
                return {
                    "success": False,
                    "message": f"任务 '{target_task_name}' 已禁用"
                }
            
            # 检查是否跳过已整理文件
            if self._skip_processed and self._is_task_processed(target_task):
                return {
                    "success": True,
                    "message": f"任务 '{target_task_name}' 已整理过，跳过处理",
                    "task_name": target_task_name,
                    "skipped": True
                }
            
            logger.info(f"API触发执行任务: {target_task_name}")
            
            # 执行SQL清理操作（静默执行，只记录日志）
            self._execute_cleanup_sql()
            
            # 执行任务
            messages = []
            success = self._execute_single_task(target_task, target_task_name, messages)
            
            result_message = messages[0] if messages else ("执行成功" if success else "执行失败")
            
            # 发送通知（如果启用）
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title=f"手动执行任务: {target_task_name}",
                    text=result_message
                )
            
            return {
                "success": success,
                "message": result_message,
                "task_name": target_task_name
            }
            
        except Exception as e:
            error_msg = f"执行任务时发生异常: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return {
                "success": False,
                "message": error_msg
            }
    
    def task_status_api(self):
        """
        API接口：获取所有任务的执行状态
        :return: 任务状态信息
        """
        try:
            self._log_scheduler("info", "API请求获取任务状态")
            
            status_info = {
                "enabled": self._enabled,
                "debug_mode": self._debug_scheduler,
                "total_tasks": len(self._tasks),
                "task_details": []
            }
            
            # 获取每个任务的状态
            for idx, task in enumerate(self._tasks):
                if not isinstance(task, dict):
                    continue
                    
                task_key = f"task_{idx}"
                task_status = self._task_status.get(task_key, {})
                
                task_info = {
                    "index": idx,
                    "name": task.get('name', f'任务{idx+1}'),
                    "enabled": task.get('enabled', True),
                    "cron": task.get('cron', '0 3 * * *'),
                    "source_path": task.get('source_path'),
                    "target_storage": task.get('target_storage'),
                    "target_path": task.get('target_path'),
                    "status": {
                        "last_run": task_status.get('last_run'),
                        "last_status": task_status.get('last_status'),
                        "last_message": task_status.get('last_message'),
                        "success_count": task_status.get('success_count', 0),
                        "fail_count": task_status.get('fail_count', 0),
                        "skip_count": task_status.get('skip_count', 0)
                    }
                }
                
                # 尝试获取下次执行时间（需要访问APScheduler）
                try:
                    from app.scheduler import scheduler
                    job_id = f"FileScanner_Task_{idx}"
                    job = scheduler.get_job(job_id)
                    if job:
                        task_info["next_run"] = job.next_run_time.isoformat() if job.next_run_time else None
                except Exception as e:
                    self._log_scheduler("debug", f"获取任务 {idx} 下次执行时间失败: {str(e)}")
                    task_info["next_run"] = None
                
                status_info["task_details"].append(task_info)
            
            self._log_scheduler("info", f"返回 {len(status_info['task_details'])} 个任务的状态信息")
            
            return {
                "success": True,
                "data": status_info
            }
            
        except Exception as e:
            error_msg = f"获取任务状态时发生异常: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self._log_scheduler("error", error_msg)
            return {
                "success": False,
                "message": error_msg
            }

    def post_message(self, mtype: NotificationType, title: str, text: str, **kwargs):
        """
        发送消息通知
        """
        eventmanager.send_event(
            etype=EventType.NoticeMessage,
            data={
                "type": mtype.value,
                "title": title,
                "text": text,
                **kwargs
            }
        )

    def _init_task_status(self, task_index: int):
        """
        初始化任务状态
        """
        task_key = f"task_{task_index}"
        if task_key not in self._task_status:
            self._task_status[task_key] = {
                "last_run": None,
                "next_run": None,
                "success_count": 0,
                "fail_count": 0,
                "skip_count": 0,
                "last_status": None,
                "last_message": None,
                "created_at": datetime.now().isoformat()
            }
            self._log_scheduler("debug", f"初始化任务状态: {task_key}")

    def _update_task_status(self, task_index: int, status: dict):
        """
        更新任务执行状态 - 增强版本
        """
        task_key = f"task_{task_index}"
        
        # 确保任务状态存在
        if task_key not in self._task_status:
            self._init_task_status(task_index)
        
        # 记录状态变更前的值
        old_status = self._task_status[task_key].get("last_status")
        old_message = self._task_status[task_key].get("last_message")
        
        # 更新状态
        self._task_status[task_key].update(status)
        
        # 记录状态变更
        new_status = status.get("last_status")
        new_message = status.get("last_message")
        
        if new_status and new_status != old_status:
            self._log_scheduler("info", f"任务状态变更 - {task_key}: {old_status} -> {new_status}")
        
        # 记录详细信息
        if status.get("last_run"):
            log_msg = f"任务状态更新 - {task_key}: status={new_status}"
            if new_message:
                log_msg += f", message={new_message}"
            if status.get("success_count"):
                log_msg += f", success_count={status['success_count']}"
            if status.get("fail_count"):
                log_msg += f", fail_count={status['fail_count']}"
            if status.get("skip_count"):
                log_msg += f", skip_count={status['skip_count']}"
            
            self._log_scheduler("info", log_msg)
            
        # 持久化任务状态（如果支持）
        try:
            # 尝试将任务状态保存到系统配置中
            from app.db.systemconfig_oper import SystemConfigOper
            from app.schemas.types import SystemConfigKey
            
            system_config = SystemConfigOper()
            status_data = {
                "task_status": self._task_status,
                "updated_at": datetime.now().isoformat()
            }
            # 使用插件特定的配置键
            config_key = f"filescanner_task_status"
            system_config.set(config_key, status_data)
            self._log_scheduler("debug", f"任务状态已持久化: {config_key}")
            
        except Exception as e:
            self._log_scheduler("debug", f"任务状态持久化失败（非关键错误）: {str(e)}")

    def _recover_task_status(self):
        """
        恢复任务状态
        """
        try:
            from app.db.systemconfig_oper import SystemConfigOper
            
            system_config = SystemConfigOper()
            config_key = f"filescanner_task_status"
            saved_status = system_config.get(config_key)
            
            if saved_status and isinstance(saved_status, dict) and "task_status" in saved_status:
                self._task_status.update(saved_status["task_status"])
                self._log_scheduler("info", f"成功恢复任务状态，共 {len(self._task_status)} 个任务")
                return True
            else:
                self._log_scheduler("debug", "没有找到保存的任务状态")
                return False
                
        except Exception as e:
            self._log_scheduler("warning", f"恢复任务状态失败: {str(e)}")
            return False

    def _init_scheduler_logger(self):
        """
        初始化独立的调度器日志记录器 - 增强版本
        """
        try:
            # 创建日志目录
            log_dir = log_settings.LOG_PATH / "plugins"
            log_dir.mkdir(parents=True, exist_ok=True)
            
            # 创建独立的日志记录器
            self._scheduler_logger = logging.getLogger(f"filescanner_scheduler")
            self._scheduler_logger.setLevel(logging.DEBUG if self._debug_scheduler else logging.INFO)
            
            # 清除已有的处理器
            self._scheduler_logger.handlers.clear()
            
            # 创建文件处理器
            log_file = log_dir / "filescanner_scheduler.log"
            file_handler = RotatingFileHandler(
                filename=log_file,
                maxBytes=5 * 1024 * 1024,  # 5MB
                backupCount=7,  # 保留7个备份文件
                encoding='utf-8'
            )
            
            # 设置日志格式
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(formatter)
            
            # 添加处理器
            self._scheduler_logger.addHandler(file_handler)
            
            # 如果是调试模式，也输出到控制台
            if self._debug_scheduler:
                console_handler = logging.StreamHandler()
                console_handler.setFormatter(formatter)
                self._scheduler_logger.addHandler(console_handler)
            
            # 禁止传播到父日志器
            self._scheduler_logger.propagate = False
            
            # 记录初始化成功
            self._scheduler_logger.info("=" * 60)
            self._scheduler_logger.info(f"FileScanner 调度器日志系统初始化成功")
            self._scheduler_logger.info(f"插件版本: {self.plugin_version}")
            self._scheduler_logger.info(f"调试模式: {'开启' if self._debug_scheduler else '关闭'}")
            self._scheduler_logger.info(f"任务数量: {len(self._tasks)}")
            self._scheduler_logger.info("=" * 60)
            
            # 尝试恢复任务状态
            self._recover_task_status()
            
        except Exception as e:
            logger.error(f"初始化调度器日志记录器失败: {str(e)}")
            # 如果初始化失败，使用默认日志器
            self._scheduler_logger = logger
    
    def _log_scheduler(self, level: str, message: str, **kwargs):
        """
        记录调度器日志 - 增强版本
        """
        if not self._scheduler_logger:
            self._scheduler_logger = logger
            
        # 确保日志记录器已初始化
        if not hasattr(self, '_scheduler_logger') or not self._scheduler_logger:
            self._scheduler_logger = logger
            
        log_method = getattr(self._scheduler_logger, level, self._scheduler_logger.info)
        
        # 添加额外的上下文信息
        if kwargs:
            try:
                import json
                extra_info = json.dumps(kwargs, ensure_ascii=False, default=str)
                message = f"{message} | 额外信息: {extra_info}"
            except:
                pass
        
        log_method(message)

    def stop_service(self):
        """
        停止插件 - 增强版本
        """
        if self._scheduler_logger:
            self._log_scheduler("info", "FileScanner 插件停止服务")
            
            # 持久化最终状态
            try:
                if hasattr(self, '_task_status') and self._task_status:
                    self._update_task_status(0, {"last_run": datetime.now().isoformat()})
            except:
                pass
            
            self._log_scheduler("info", "=" * 60)
