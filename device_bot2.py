import asyncio
import json
import os
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple, Callable, Any
from dataclasses import dataclass, field

from nio import (
    AsyncClient, 
    RoomMessageText, 
    LoginResponse,
    InviteEvent,
)

from config import (
    HOMESERVER, USERNAME, PASSWORD, COMMAND_PREFIX,
    DATA_FILE, SUBSCRIPTIONS_FILE, SUBSCRIPTION_ROOMS_FILE,
    SYNC_TOKEN_FILE, SYNC_TIMEOUT, LOCALPART_DOMAIN
)


# ============================================================================
# State
# ============================================================================

class DeviceState(Enum):
    FREE = "free"
    OCCUPIED = "occupied"
    MAINTENANCE = "maintenance"


class BotState(Enum):
    RUNNING = "running"
    STOPPED = "stopped"
    CONNECTING = "connecting"
    ERROR = "error"


@dataclass
class Device:
    """Device entity with state management"""
    name: str
    state: DeviceState = DeviceState.FREE
    users: List[str] = field(default_factory=list)
    occupied_at: Optional[str] = None
    room_id: Optional[str] = None
    
    def occupy(self, user_id: str, room_id: str) -> bool:
        if self.state != DeviceState.FREE:
            return False
        self.state = DeviceState.OCCUPIED
        self.users = [user_id]
        self.occupied_at = datetime.now().isoformat()
        self.room_id = room_id
        return True
    
    def free(self, user_id: str) -> bool:
        if user_id not in self.users:
            return False
        self.users.remove(user_id)
        if not self.users:
            self.state = DeviceState.FREE
            self.occupied_at = None
            self.room_id = None
        return True
    
    def kick_user(self, user_id: str) -> bool:
        if user_id not in self.users:
            return False
        self.users.remove(user_id)
        if not self.users:
            self.state = DeviceState.FREE
            self.occupied_at = None
            self.room_id = None
        return True
    
    def is_occupied_by(self, user_id: str) -> bool:
        return user_id in self.users
    
    @property
    def owners(self) -> List[str]:
        return [u.split(":")[0].replace("@", "") for u in self.users]
    
    def get_occupied_time(self) -> Optional[str]:
        """Get formatted occupied time"""
        if not self.occupied_at:
            return None
        try:
            occupied_dt = datetime.fromisoformat(self.occupied_at)
            now = datetime.now()
            delta = now - occupied_dt
            hours = delta.seconds // 3600
            minutes = (delta.seconds % 3600) // 60
            if delta.days > 0:
                return f"{delta.days}д {hours}ч"
            elif hours > 0:
                return f"{hours}ч {minutes}мин"
            else:
                return f"{minutes}мин"
        except:
            return None


# ============================================================================
# Command - Generic Command
# ============================================================================

@dataclass
class CommandContext:
    """Context for command execution"""
    room_id: str
    sender: str
    args: List[str]
    raw_command: str


class Command:
    """Generic command class with variable-length ambiguous matching"""
    
    def __init__(
        self,
        names: List[str],
        description: str,
        handler: Callable,
        min_args: int = 0,
        admin_only: bool = False
    ):
        self.names = names
        self.description = description
        self.handler = handler
        self.min_args = min_args
        self.admin_only = admin_only
    
    def matches(self, input_name: str) -> bool:
        """Check if command matches with partial matching support"""
        input_lower = input_name.lower()
        for name in self.names:
            if name == input_lower or (len(input_lower) >= 1 and name.startswith(input_lower)):
                return True
        return False
    
    async def execute(self, bot: 'DeviceBot', context: CommandContext) -> None:
        """Execute the command with validation"""
        if len(context.args) < self.min_args:
            await bot.send_message(
                context.room_id,
                f"❌ Недостаточно аргументов. Использование: `!{self.names[0]} ...`\n{self.description}"
            )
            return
        
        await self.handler(bot, context)


class CommandRegistry:
    """Registry for managing commands with ambiguous resolution"""
    
    def __init__(self):
        self.commands: List[Command] = []
        self._command_map: Dict[str, Command] = {}
    
    def register(self, command: Command) -> None:
        """Register a command"""
        self.commands.append(command)
        for name in command.names:
            self._command_map[name] = command
    
    def resolve_command(self, input_name: str) -> Optional[Tuple[Command, str]]:
        """
        Resolve command allowing variable length inputs until unambiguous.
        Returns (command, matched_name) or None
        """
        input_lower = input_name.lower()
        
        # Try exact match first
        if input_lower in self._command_map:
            return (self._command_map[input_lower], input_lower)
        
        # Find all partial matches
        matches = []
        for cmd in self.commands:
            for name in cmd.names:
                if name.startswith(input_lower):
                    matches.append((cmd, name))
                    break
        
        # Return unique match
        if len(matches) == 1:
            return matches[0]
        
        return None


# ============================================================================
# Observer - Subscription Manager
# ============================================================================

class SubscriptionManager:
    """Manages subscriptions"""
    
    def __init__(self, bot: 'DeviceBot'):
        self.bot = bot
        self.subscriptions: Dict[str, List[str]] = {}  # device -> users
        self.subscription_rooms: Dict[str, Dict[str, str]] = {}  # user -> device -> room
        self._load()
    
    def _load(self):
        """Load subscriptions from file"""
        if os.path.exists(SUBSCRIPTIONS_FILE):
            try:
                with open(SUBSCRIPTIONS_FILE, 'r') as f:
                    self.subscriptions = json.load(f)
                print(f"✅ Loaded subscriptions: {len(self.subscriptions)}")
            except:
                self.subscriptions = {}
        
        if os.path.exists(SUBSCRIPTION_ROOMS_FILE):
            try:
                with open(SUBSCRIPTION_ROOMS_FILE, 'r') as f:
                    self.subscription_rooms = json.load(f)
            except:
                self.subscription_rooms = {}
    
    def _save(self):
        """Save subscriptions to file"""
        with open(SUBSCRIPTIONS_FILE, 'w') as f:
            json.dump(self.subscriptions, f, indent=2, ensure_ascii=False)
        with open(SUBSCRIPTION_ROOMS_FILE, 'w') as f:
            json.dump(self.subscription_rooms, f, indent=2, ensure_ascii=False)
    
    def subscribe(self, device_name: str, user_id: str, room_id: str) -> bool:
        """Subscribe user to device notifications"""
        if device_name not in self.subscriptions:
            self.subscriptions[device_name] = []
        
        if user_id in self.subscriptions[device_name]:
            return False
        
        self.subscriptions[device_name].append(user_id)
        
        if user_id not in self.subscription_rooms:
            self.subscription_rooms[user_id] = {}
        self.subscription_rooms[user_id][device_name] = room_id
        
        self._save()
        return True
    
    def unsubscribe(self, device_name: str, user_id: str) -> bool:
        """Unsubscribe user from device"""
        if device_name not in self.subscriptions:
            return False
        
        if user_id not in self.subscriptions[device_name]:
            return False
        
        self.subscriptions[device_name].remove(user_id)
        
        if user_id in self.subscription_rooms and device_name in self.subscription_rooms[user_id]:
            del self.subscription_rooms[user_id][device_name]
            if not self.subscription_rooms[user_id]:
                del self.subscription_rooms[user_id]
        
        self._save()
        return True
    
    def get_subscribers(self, device_name: str) -> List[str]:
        """Get list of subscribers for a device"""
        return self.subscriptions.get(device_name, [])
    
    def get_user_subscriptions(self, user_id: str) -> List[str]:
        """Get devices user is subscribed to"""
        return [device for device, users in self.subscriptions.items() if user_id in users]
    
    def get_subscription_room(self, user_id: str, device_name: str) -> Optional[str]:
        """Get room ID for a user's subscription to a device"""
        return self.subscription_rooms.get(user_id, {}).get(device_name)
    
    async def notify_subscribers(self, device_name: str, freed_by: Optional[str] = None):
        """Notify all subscribers about device availability"""
        subscribers = self.get_subscribers(device_name)
        if not subscribers:
            return
        
        freed_by_name = freed_by.split(":")[0].replace("@", "") if freed_by else "кем-то"
        
        for subscriber in subscribers:
            room_id = self.get_subscription_room(subscriber, device_name)
            if room_id:
                message = f"🔔 Устройство **{device_name}** освободилось!\n\nОсвободил: **{freed_by_name}**"
                await self.bot.send_message(room_id, message)
                self.unsubscribe(device_name, subscriber)


# ============================================================================
# Main Bot Class
# ============================================================================

class DeviceBot:
    """Main bot class"""
    
    def __init__(self):
        self.client: Optional[AsyncClient] = None
        self.devices: Dict[str, Device] = {}
        self.state: BotState = BotState.STOPPED
        self.subscription_manager: Optional[SubscriptionManager] = None
        self.command_registry: CommandRegistry = CommandRegistry()
        self.next_batch: Optional[str] = None
        self.user_rooms: Dict[str, str] = {}
        
        self._load_data()
        self._register_commands()
    
    def _load_data(self):
        """Load all persistent data"""
        self._load_devices()
        self.subscription_manager = SubscriptionManager(self)
        self._load_batch_token()
    
    def _load_devices(self):
        """Load devices from file"""
        if os.path.exists(DATA_FILE):
            try:
                with open(DATA_FILE, 'r') as f:
                    data = json.load(f)
                    for name, info in data.items():
                        state_value = info.get("state", "free")
                        state = DeviceState(state_value) if state_value in [s.value for s in DeviceState] else DeviceState.FREE
                        
                        self.devices[name] = Device(
                            name=name,
                            state=state,
                            users=info.get("users", []),
                            occupied_at=info.get("occupied_at"),
                            room_id=info.get("room_id")
                        )
                print(f"✅ Loaded {len(self.devices)} devices")
            except Exception as e:
                print(f"Error loading devices: {e}")
                self.devices = {}
    
    def _save_devices(self):
        """Save devices to file"""
        data = {}
        for name, device in self.devices.items():
            data[name] = {
                "state": device.state.value,
                "users": device.users,
                "occupied_at": device.occupied_at,
                "room_id": device.room_id
            }
        with open(DATA_FILE, 'w') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    
    def _load_batch_token(self):
        """Load sync token from file"""
        if os.path.exists(SYNC_TOKEN_FILE):
            try:
                with open(SYNC_TOKEN_FILE, 'r') as f:
                    self.next_batch = f.read().strip()
            except:
                pass
    
    def _save_batch_token(self):
        """Save sync token to file"""
        if self.next_batch:
            with open(SYNC_TOKEN_FILE, 'w') as f:
                f.write(self.next_batch)
    
    def _register_commands(self):
        """Register all commands with their handlers"""
        
        # !list or !l or !ls - Show all devices
        self.command_registry.register(Command(
            names=["list", "l", "ls"],
            description="Показать все устройства",
            handler=self._handle_device_list
        ))
        
        # !get or !g - Occupy device
        self.command_registry.register(Command(
            names=["get", "g"],
            description="Занять устройство. Использование: !g <устройство>",
            handler=self._handle_device_get,
            min_args=1
        ))
        
        # !free or !f - Free device
        self.command_registry.register(Command(
            names=["free", "f"],
            description="Освободить устройство. Использование: !free <устройство>",
            handler=self._handle_device_free,
            min_args=1
        ))
        
        # !mydevices or !m - Show my devices
        self.command_registry.register(Command(
            names=["mydevices", "m"],
            description="Показать мои устройства",
            handler=self._handle_my_devices
        ))
        
        # !subscribe or !s - Subscribe to device
        self.command_registry.register(Command(
            names=["subscribe", "s"],
            description="Подписаться на уведомления. Использование: !s <устройство>",
            handler=self._handle_subscribe,
            min_args=1
        ))
        
        # !unsubscribe or !u - Unsubscribe from device
        self.command_registry.register(Command(
            names=["unsubscribe", "u"],
            description="Отписаться от уведомлений. Использование: !u <устройство>",
            handler=self._handle_unsubscribe,
            min_args=1
        ))
        
        # !subscriptions - Show my subscriptions
        self.command_registry.register(Command(
            names=["subscriptions"],
            description="Показать мои подписки",
            handler=self._handle_subscriptions
        ))
        
        # !subscribers or !ss - Show device subscribers
        self.command_registry.register(Command(
            names=["subscribers", "ss"],
            description="Показать подписчиков устройства. Использование: !ss <устройство>",
            handler=self._handle_subscribers,
            min_args=1
        ))
        
        # !add or !a - Add device
        self.command_registry.register(Command(
            names=["add", "a", "new", "n"],
            description="Добавить устройство. Использование: !a <устройство>",
            handler=self._handle_add_device,
            min_args=1,
            admin_only=True
        ))
        
        # !remove or !r or !rm - Remove device
        self.command_registry.register(Command(
            names=["remove", "r", "rm"],
            description="Удалить устройство. Использование: !r <устройство>",
            handler=self._handle_remove_device,
            min_args=1,
            admin_only=True
        ))
        
        # !kick or !k - Kick user from device
        self.command_registry.register(Command(
            names=["kick", "k"],
            description="Кикнуть пользователя с устройства. Использование: !k <устройство> [пользователь]",
            handler=self._handle_kick,
            min_args=1,
            admin_only=True
        ))
        
        # !kickall or !ka - Free all devices
        self.command_registry.register(Command(
            names=["kickall", "ka"],
            description="Освободить все устройства",
            handler=self._handle_kick_all,
            admin_only=True
        ))
        
        # !menu - Show menu
        self.command_registry.register(Command(
            names=["menu"],
            description="Показать главное меню",
            handler=self._handle_menu
        ))
        
        # !help or !h - Show help
        self.command_registry.register(Command(
            names=["help", "h"],
            description="Показать справку",
            handler=self._handle_help
        ))
    
    # Command handlers
    async def _handle_device_list(self, bot: 'DeviceBot', ctx: CommandContext) -> None:
        """Handle device list command"""
        await bot.send_device_list(ctx.room_id)
    
    async def _handle_device_get(self, bot: 'DeviceBot', ctx: CommandContext) -> None:
        """Handle get device command"""
        device_name = " ".join(ctx.args)
        
        if device_name not in bot.devices:
            await bot.send_message(ctx.room_id, f"❌ Устройство **{device_name}** не найдено\nВведите `!list` для списка")
            return
        
        device = bot.devices[device_name]
        
        if device.state != DeviceState.FREE:
            owners = device.owners
            await bot.send_message(ctx.room_id, f"❌ Устройство **{device_name}** уже занято: **{', '.join(owners)}**")
            return
        
        if bot.occupy_device(device_name, ctx.sender, ctx.room_id):
            await bot.send_message(ctx.room_id, f"✅ Вы заняли устройство **{device_name}**")
            await bot.send_device_controls(ctx.room_id, device_name, ctx.sender)
        else:
            await bot.send_message(ctx.room_id, f"❌ Не удалось занять устройство **{device_name}**")
    
    async def _handle_device_free(self, bot: 'DeviceBot', ctx: CommandContext) -> None:
        """Handle free device command"""
        device_name = " ".join(ctx.args)
        
        if bot.free_device(device_name, ctx.sender):
            await bot.send_message(ctx.room_id, f"✅ Вы освободили устройство **{device_name}**")
            await bot.notify_subscribers(device_name, ctx.sender)
            await bot.send_device_list(ctx.room_id)
        else:
            await bot.send_message(ctx.room_id, f"❌ Вы не занимаете устройство **{device_name}**")
    
    async def _handle_my_devices(self, bot: 'DeviceBot', ctx: CommandContext) -> None:
        """Handle my devices command"""
        user_devices = []
        for name, device in bot.devices.items():
            if device.is_occupied_by(ctx.sender):
                user_devices.append((name, device))
        
        if user_devices:
            text = "📱 **Ваши устройства:**\n\n"
            for name, device in user_devices:
                occupied_time = device.get_occupied_time()
                time_str = f" (занято {occupied_time})" if occupied_time else ""
                text += f"• `{name}`{time_str} — `!free {name}`\n"
            await bot.send_message(ctx.room_id, text)
        else:
            await bot.send_message(ctx.room_id, "📭 Вы не занимаете ни одного устройства")
    
    async def _handle_subscribe(self, bot: 'DeviceBot', ctx: CommandContext) -> None:
        """Handle subscribe command"""
        device_name = " ".join(ctx.args)
        
        if device_name not in bot.devices:
            await bot.send_message(ctx.room_id, f"❌ Устройство **{device_name}** не найдено")
            return
        
        if bot.subscribe(device_name, ctx.sender, ctx.room_id):
            await bot.send_message(ctx.room_id, f"✅ Вы подписались на уведомления об освобождении **{device_name}**")
        else:
            await bot.send_message(ctx.room_id, f"❌ Вы уже подписаны на **{device_name}**")
    
    async def _handle_unsubscribe(self, bot: 'DeviceBot', ctx: CommandContext) -> None:
        """Handle unsubscribe command"""
        device_name = " ".join(ctx.args)
        
        if bot.unsubscribe(device_name, ctx.sender):
            await bot.send_message(ctx.room_id, f"✅ Вы отписались от уведомлений **{device_name}**")
        else:
            await bot.send_message(ctx.room_id, f"❌ Вы не были подписаны на **{device_name}**")
    
    async def _handle_subscriptions(self, bot: 'DeviceBot', ctx: CommandContext) -> None:
        """Handle subscriptions list command"""
        subscribed = bot.subscription_manager.get_user_subscriptions(ctx.sender)
        if subscribed:
            text = "📋 **Ваши подписки:**\n\n"
            for device in subscribed:
                text += f"• `{device}`\n"
            await bot.send_message(ctx.room_id, text)
        else:
            await bot.send_message(ctx.room_id, "📭 Вы не подписаны ни на одно устройство")
    
    async def _handle_subscribers(self, bot: 'DeviceBot', ctx: CommandContext) -> None:
        """Handle show device subscribers command"""
        device_name = " ".join(ctx.args)
        
        if device_name not in bot.devices:
            await bot.send_message(ctx.room_id, f"❌ Устройство **{device_name}** не найдено")
            return
        
        subscribers = bot.subscription_manager.get_subscribers(device_name)
        if subscribers:
            subscriber_names = [s.split(":")[0].replace("@", "") for s in subscribers]
            text = f"👥 **Подписчики устройства {device_name}:**\n\n"
            for name in subscriber_names:
                text += f"• {name}\n"
            await bot.send_message(ctx.room_id, text)
        else:
            await bot.send_message(ctx.room_id, f"📭 На устройство **{device_name}** никто не подписан")
    
    async def _handle_add_device(self, bot: 'DeviceBot', ctx: CommandContext) -> None:
        """Handle add device command"""
        device_name = " ".join(ctx.args)
        
        if bot.add_device(device_name):
            await bot.send_message(ctx.room_id, f"✅ Устройство **{device_name}** добавлено")
            await bot.send_device_list(ctx.room_id)
        else:
            await bot.send_message(ctx.room_id, f"❌ Устройство **{device_name}** уже существует")
    
    async def _handle_remove_device(self, bot: 'DeviceBot', ctx: CommandContext) -> None:
        """Handle remove device command"""
        device_name = " ".join(ctx.args)
        
        if bot.remove_device(device_name):
            await bot.send_message(ctx.room_id, f"✅ Устройство **{device_name}** удалено")
            await bot.send_device_list(ctx.room_id)
        else:
            await bot.send_message(ctx.room_id, f"❌ Устройство **{device_name}** не найдено")
    
    async def _handle_kick(self, bot: 'DeviceBot', ctx: CommandContext) -> None:
        """Handle kick command"""
        device_name = ctx.args[0]
        target_user = ctx.args[1] if len(ctx.args) > 1 else None
        
        if device_name not in bot.devices:
            await bot.send_message(ctx.room_id, f"❌ Устройство **{device_name}** не найдено")
            return
        
        device = bot.devices[device_name]
        
        if device.state == DeviceState.FREE:
            await bot.send_message(ctx.room_id, f"❌ Устройство **{device_name}** уже свободно")
            return
        
        # Determine target user
        if target_user:
            # Check if target_user already has @ and domain
            if ":" in target_user:
                target_user_id = target_user
            else:
                target_user_id = f"@{target_user}:{LOCALPART_DOMAIN}"
            
            if not device.is_occupied_by(target_user_id):
                await bot.send_message(ctx.room_id, f"❌ Пользователь **{target_user}** не занимает устройство **{device_name}**")
                return
            kicked_user_id = target_user_id
        else:
            kicked_user_id = device.users[0] if device.users else None
            if not kicked_user_id:
                await bot.send_message(ctx.room_id, f"❌ Нет пользователя для кика с устройства **{device_name}**")
                return
        
        # Save room_id before kicking
        user_room_id = device.room_id
        
        if device.kick_user(kicked_user_id):
            bot._save_devices()
            
            admin_name = ctx.sender.split(":")[0].replace("@", "")
            kicked_name = kicked_user_id.split(":")[0].replace("@", "")
            
            await bot.send_message(
                ctx.room_id,
                f"✅ Пользователь **{kicked_name}** кикнут с устройства **{device_name}** по запросу **{admin_name}**"
            )
            
            # Send notification to kicked user
            if user_room_id:
                await bot.send_message(
                    user_room_id,
                    f"⚠️ **Вас кикнули с устройства {device_name}**\n\nПо запросу пользователя **{admin_name}**"
                )
                print(f"📨 Уведомление отправлено в комнату {user_room_id}")
            else:
                await bot.send_notification(
                    kicked_user_id,
                    f"⚠️ Вас кикнули с устройства **{device_name}**\n\nПо запросу пользователя **{admin_name}**"
                )
            
            await bot.subscription_manager.notify_subscribers(device_name, ctx.sender)
            await bot.send_device_list(ctx.room_id)
        else:
            await bot.send_message(ctx.room_id, f"❌ Не удалось кикнуть пользователя")
    
    async def _handle_kick_all(self, bot: 'DeviceBot', ctx: CommandContext) -> None:
        """Handle kickall command"""
        kicked_count = 0
        kicked_users = []
        
        for device_name, device in bot.devices.items():
            if device.state == DeviceState.OCCUPIED:
                for kicked_user in device.users:
                    kicked_users.append((kicked_user, device_name, device.room_id))
                    kicked_count += 1
                device.users = []
                device.state = DeviceState.FREE
                device.occupied_at = None
                device.room_id = None
        
        bot._save_devices()
        
        if kicked_count > 0:
            admin_name = ctx.sender.split(":")[0].replace("@", "")
            await bot.send_message(ctx.room_id, f"✅ **Kickall выполнен!** Освобождено устройств: {kicked_count}")
            
            for kicked_user, device_name, room_id in kicked_users:
                kicked_name = kicked_user.split(":")[0].replace("@", "")
                if room_id:
                    await bot.send_message(
                        room_id,
                        f"⚠️ **Вас кикнули с устройства {device_name}**\n\nПо запросу **{admin_name}** (команда kickall)"
                    )
                else:
                    await bot.send_notification(
                        kicked_user,
                        f"⚠️ Вас кикнули с устройства **{device_name}**\n\nПо запросу **{admin_name}** (команда kickall)"
                    )
            
            for device_name in bot.devices:
                await bot.subscription_manager.notify_subscribers(device_name, ctx.sender)
            
            await bot.send_device_list(ctx.room_id)
        else:
            await bot.send_message(ctx.room_id, "❌ Нет занятых устройств для освобождения")
    
    async def _handle_menu(self, bot: 'DeviceBot', ctx: CommandContext) -> None:
        """Handle menu command"""
        await bot.send_main_menu(ctx.room_id)
    
    async def _handle_help(self, bot: 'DeviceBot', ctx: CommandContext) -> None:
        """Handle help command"""
        help_text = "🤖 **Команды бота:**\n\n"
        
        for cmd in bot.command_registry.commands:
            help_text += f"• `!{cmd.names[0]}`"
            if len(cmd.names) > 1:
                aliases = [f"!{n}" for n in cmd.names[1:] if n != cmd.names[0]]
                if aliases:
                    help_text += f" ({', '.join(aliases)})"
            help_text += f" — {cmd.description}\n"
        
        help_text += "\n**Доступные устройства:**\n\n"
        for name, device in bot.devices.items():
            if device.state == DeviceState.FREE:
                help_text += f"• `{name}` — ✅ свободно\n"
            else:
                occupied_time = device.get_occupied_time()
                time_str = f" (занято {occupied_time})" if occupied_time else ""
                help_text += f"• `{name}` — 🔴 занято: {', '.join(device.owners)}{time_str}\n"
        
        await bot.send_message(ctx.room_id, help_text)
    
    # Core bot methods
    def occupy_device(self, device_name: str, user_id: str, room_id: str) -> bool:
        """Occupy a device"""
        if device_name not in self.devices:
            return False
        
        device = self.devices[device_name]
        if device.occupy(user_id, room_id):
            self._save_devices()
            return True
        return False
    
    def free_device(self, device_name: str, user_id: str) -> bool:
        """Free a device"""
        if device_name not in self.devices:
            return False
        
        device = self.devices[device_name]
        if device.free(user_id):
            self._save_devices()
            return True
        return False
    
    def add_device(self, device_name: str) -> bool:
        """Add a new device"""
        if device_name in self.devices:
            return False
        
        self.devices[device_name] = Device(name=device_name)
        self._save_devices()
        return True
    
    def remove_device(self, device_name: str) -> bool:
        """Remove a device"""
        if device_name not in self.devices:
            return False
        
        del self.devices[device_name]
        self._save_devices()
        return True
    
    def subscribe(self, device_name: str, user_id: str, room_id: str) -> bool:
        """Subscribe to device notifications"""
        return self.subscription_manager.subscribe(device_name, user_id, room_id)
    
    def unsubscribe(self, device_name: str, user_id: str) -> bool:
        """Unsubscribe from device"""
        return self.subscription_manager.unsubscribe(device_name, user_id)
    
    async def notify_subscribers(self, device_name: str, freed_by: Optional[str] = None):
        """Notify subscribers"""
        await self.subscription_manager.notify_subscribers(device_name, freed_by)
    
    async def send_message(self, room_id: str, message: str):
        """Send a simple message"""
        try:
            await self.client.room_send(
                room_id=room_id,
                message_type="m.room.message",
                content={"msgtype": "m.text", "body": message}
            )
        except Exception as e:
            print(f"Error sending message: {e}")
    
    async def send_notification(self, user_id: str, message: str):
        """Send notification to user"""
        try:
            if user_id in self.user_rooms:
                await self.send_message(self.user_rooms[user_id], message)
                return
            
            response = await self.client.room_create(
                invite=[user_id],
                visibility="private",
                name="Уведомления от бота устройств"
            )
            
            if hasattr(response, 'room_id'):
                room_id = response.room_id
                self.user_rooms[user_id] = room_id
                await self.send_message(room_id, "👋 Привет! Я бот управления устройствами.")
                await self.send_message(room_id, message)
                print(f"✅ Создана комната уведомлений для {user_id}")
            else:
                print(f"❌ Не удалось создать комнату для {user_id}")
        except Exception as e:
            print(f"Ошибка уведомления: {e}")
    
    async def send_device_list(self, room_id: str):
        """Send device list"""
        if not self.devices:
            text = "📭 Список устройств пуст. Добавьте устройства командой `!add <название>`"
        else:
            text = "**📱 Список устройств:**\n\n"
            for name, device in self.devices.items():
                if device.state == DeviceState.FREE:
                    text += f"• **{name}** — ✅ Свободно\n"
                else:
                    occupied_time = device.get_occupied_time()
                    time_str = f" (занято {occupied_time})" if occupied_time else ""
                    text += f"• **{name}** — 🔴 Занято: {', '.join(device.owners)}{time_str}\n"
            
            text += "\n💡 **Как занять устройство:**\n"
            available = [name for name, d in self.devices.items() if d.state == DeviceState.FREE]
            if available:
                text += "Скопируйте и отправьте команду:\n"
                for device in available[:3]:
                    text += f"• `!g {device}`\n"
                if len(available) > 3:
                    text += f"• и ещё {len(available) - 3} устройств\n"
            else:
                text += "❌ Нет свободных устройств\n"
            
            text += "\n**Другие команды:**\n"
            text += "• `!mydevices` — мои устройства\n"
            text += "• `!help` — все команды"
        
        await self.send_message(room_id, text)
    
    async def send_device_controls(self, room_id: str, device_name: str, user_id: str):
        """Send device controls"""
        device = self.devices.get(device_name)
        if not device:
            return
        
        is_owner = device.is_occupied_by(user_id)
        status = "🔴 Занято" if device.state == DeviceState.OCCUPIED else "✅ Свободно"
        
        text = f"**🖥️ {device_name}**\n"
        text += f"Статус: {status}\n"
        text += f"Владельцы: {', '.join(device.owners) if device.owners else 'никем'}\n"
        
        occupied_time = device.get_occupied_time()
        if occupied_time:
            text += f"Время занятия: {occupied_time}\n"
        
        text += "\n"
        
        if is_owner:
            text += f"🔓 Чтобы освободить: `!free {device_name}`\n\n"
        
        text += "**Другие команды:**\n"
        text += "• `!list` — список устройств\n"
        text += "• `!mydevices` — мои устройства"
        
        await self.send_message(room_id, text)
    
    async def send_main_menu(self, room_id: str):
        """Send main menu"""
        text = """🤖 **Бот управления устройствами**

**Основные команды:**
• `!list` (`!l`) — показать все устройства
• `!get` (`!g`) — занять устройство
• `!free` (`!f`) — освободить устройство
• `!mydevices` (`!m`) — показать мои устройства

**Подписки:**
• `!subscribe` (`!s`) — подписаться на освобождение
• `!unsubscribe` (`!u`) — отписаться
• `!subscriptions` — список подписок
• `!subscribers` (`!ss`) — подписчики устройства

**Административные:**
• `!add` (`!a`) — добавить устройство
• `!remove` (`!r`, `!rm`) — удалить устройство
• `!kick` (`!k`) — кикнуть пользователя
• `!kickall` (`!ka`) — освободить ВСЕ устройства

Введите команду, чтобы начать!"""
        
        await self.send_message(room_id, text)
    
    async def handle_command(self, room_id: str, sender: str, command: str):
        """Handle commands with ambiguous resolution"""
        parts = command.strip().split()
        if not parts:
            return
        
        cmd_name = parts[0].lower()
        result = self.command_registry.resolve_command(cmd_name)
        
        if result:
            command_obj, matched_name = result
            args = parts[1:] if len(parts) > 1 else []
            
            context = CommandContext(
                room_id=room_id,
                sender=sender,
                args=args,
                raw_command=command
            )
            
            await command_obj.execute(self, context)
        else:
            await self.send_message(
                room_id,
                f"❌ Неизвестная команда: `{cmd_name}`\nВведите `!help` для списка команд"
            )
    
    async def run(self):
        """Run the bot"""
        print(f"🚀 Запуск бота {USERNAME} на сервере {HOMESERVER}")
        
        self.client = AsyncClient(HOMESERVER, USERNAME)
        self.state = BotState.CONNECTING
        
        response = await self.client.login(PASSWORD)
        
        if isinstance(response, LoginResponse):
            print(f"✅ Бот успешно залогинен как {USERNAME}")
            print(f"✅ User ID: {self.client.user_id}")
            self.state = BotState.RUNNING
        else:
            print(f"❌ Ошибка логина: {response}")
            self.state = BotState.ERROR
            return
        
        print("🤖 Бот запущен и готов к работе!")
        print("💡 Пригласите бота в комнату и напишите !help")
        
        try:
            while self.state == BotState.RUNNING:
                try:
                    sync_response = await self.client.sync(timeout=SYNC_TIMEOUT, since=self.next_batch)
                    
                    self.next_batch = sync_response.next_batch
                    self._save_batch_token()
                    
                    # Handle invites
                    if hasattr(sync_response, 'rooms') and hasattr(sync_response.rooms, 'invite'):
                        for room_id in sync_response.rooms.invite:
                            try:
                                await self.client.join(room_id)
                                await self.send_main_menu(room_id)
                            except Exception as e:
                                print(f"Ошибка присоединения: {e}")
                    
                    # Handle messages
                    if hasattr(sync_response, 'rooms') and hasattr(sync_response.rooms, 'join'):
                        for room_id, room_data in sync_response.rooms.join.items():
                            for event in room_data.timeline.events:
                                if isinstance(event, RoomMessageText) and event.sender != self.client.user_id:
                                    body = event.body.strip()
                                    if body.startswith(COMMAND_PREFIX):
                                        command = body[len(COMMAND_PREFIX):]
                                        await self.handle_command(room_id, event.sender, command)
                
                except Exception as e:
                    print(f"Ошибка синхронизации: {e}")
                    await asyncio.sleep(5)
        
        except KeyboardInterrupt:
            print("\n🛑 Бот остановлен")
            self.state = BotState.STOPPED
        finally:
            await self.client.close()


if __name__ == "__main__":
    bot = DeviceBot()
    asyncio.run(bot.run())
