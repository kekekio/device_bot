import asyncio
import json
import os
import logging
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Tuple, Callable, Any
from dataclasses import dataclass, field

# Disable verbose nio logging
logging.getLogger("nio").setLevel(logging.WARNING)
logging.getLogger("nio.client.base_client").setLevel(logging.WARNING)
logging.getLogger("nio.rooms").setLevel(logging.WARNING)

# Configure logging for bot
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

from nio import (
    AsyncClient,
    RoomMessageText,
    LoginResponse,
    InviteEvent,
)

from config import (
    HOMESERVER,
    USERNAME,
    PASSWORD,
    COMMAND_PREFIX,
    DATA_FILE,
    SYNC_TOKEN_FILE,
    SYNC_TIMEOUT,
    LOCALPART_DOMAIN,
)

# ============================================================================
# Device State Enum (internal use)
# ============================================================================


class _DeviceState(Enum):
    FREE = "free"
    OCCUPIED = "occupied"
    MAINTENANCE = "maintenance"
    RESERVED = "reserved"


class _BotState(Enum):
    RUNNING = "running"
    STOPPED = "stopped"
    CONNECTING = "connecting"
    ERROR = "error"


class OperationResult:
    """Result of device operation"""

    def __init__(self, success: bool, message: str, data: Any = None):
        self.success = success
        self.message = message
        self.data = data

    @classmethod
    def ok(cls, message: str, data: Any = None) -> "OperationResult":
        return cls(True, message, data)

    @classmethod
    def error(cls, message: str) -> "OperationResult":
        return cls(False, message)


# ============================================================================
# Device Class with all device logic
# ============================================================================


@dataclass
class Device:
    name: str
    state: _DeviceState = _DeviceState.FREE
    users: List[str] = field(default_factory=list)
    occupied_at: Optional[str] = None
    room_id: Optional[str] = None
    reserved_for: Optional[str] = None
    reserved_by: Optional[str] = None
    reserved_at: Optional[str] = None
    subscriptions: List[str] = field(default_factory=list)
    subscription_rooms: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        """Ensure state is DeviceState enum"""
        if isinstance(self.state, str):
            try:
                self.state = _DeviceState(self.state)
            except ValueError:
                self.state = _DeviceState.FREE

    def occupy(self, user_id: str, room_id: str) -> OperationResult:
        """Occupy device (only if FREE or RESERVED for this user)"""
        if self.state == _DeviceState.OCCUPIED:
            owners = self.owners
            return OperationResult.error(
                f"Устройство {self.name} уже занято: {', '.join(owners)}"
            )

        if self.state == _DeviceState.RESERVED:
            if self.reserved_for != user_id:
                reserve_info = self.get_reserve_info()
                return OperationResult.error(f"Устройство {self.name} {reserve_info}")

            self.state = _DeviceState.OCCUPIED
            self.users = [user_id]
            self.occupied_at = datetime.now().isoformat()
            self.room_id = room_id
            return OperationResult.ok(
                f"Вы заняли устройство {self.name} (было зарезервировано для вас)"
            )

        if self.state == _DeviceState.MAINTENANCE:
            return OperationResult.error(f"Устройство {self.name} на обслуживании")

        self.state = _DeviceState.OCCUPIED
        self.users = [user_id]
        self.occupied_at = datetime.now().isoformat()
        self.room_id = room_id
        return OperationResult.ok(f"Вы заняли устройство {self.name}")

    def free(self, user_id: str) -> OperationResult:
        """Free device from a user"""
        if user_id not in self.users:
            return OperationResult.error(f"Вы не занимаете устройство {self.name}")

        self.users.remove(user_id)
        if not self.users:
            if self.reserved_for:
                self.state = _DeviceState.RESERVED
            else:
                self.state = _DeviceState.FREE
            self.occupied_at = None
            self.room_id = None

        return OperationResult.ok(f"Вы освободили устройство {self.name}")

    def kick_user(self, admin_id: str, target_user_id: str) -> OperationResult:
        """Kick a user from device (cannot kick reserved user)"""
        if self.state == _DeviceState.FREE:
            return OperationResult.error(f"Устройство {self.name} уже свободно")

        if target_user_id == self.reserved_for:
            target_name = target_user_id.split(":")[0].replace("@", "")
            return OperationResult.error(
                f"Нельзя кикнуть {target_name} - устройство зарезервировано для него"
            )

        if target_user_id not in self.users:
            target_name = target_user_id.split(":")[0].replace("@", "")
            return OperationResult.error(
                f"Пользователь {target_name} не занимает устройство {self.name}"
            )

        admin_name = admin_id.split(":")[0].replace("@", "")
        kicked_name = target_user_id.split(":")[0].replace("@", "")

        self.users.remove(target_user_id)
        room_id = self.room_id
        if not self.users:
            if self.reserved_for:
                self.state = _DeviceState.RESERVED
            else:
                self.state = _DeviceState.FREE
            self.occupied_at = None
            self.room_id = None

        return OperationResult.ok(
            (f"Пользователь {kicked_name} кикнут "
                f"с устройства {self.name} по запросу {admin_name}"),
            {"kicked_user": target_user_id, "room_id": room_id},
        )

    def reserve(self, user_id: str, target_user_id: str) -> OperationResult:
        """Reserve device for target_user"""
        if self.state == _DeviceState.OCCUPIED:
            owners = self.owners
            return OperationResult.error(
                f"Устройство {self.name} уже занято: {', '.join(owners)}"
            )

        if self.state == _DeviceState.MAINTENANCE:
            return OperationResult.error(f"Устройство {self.name} на обслуживании")

        if self.state == _DeviceState.RESERVED and self.reserved_by != user_id:
            reserve_info = self.get_reserve_info()
            return OperationResult.error(f"Устройство {self.name} уже {reserve_info}")

        target_name = target_user_id.split(":")[0].replace("@", "")
        user_name = user_id.split(":")[0].replace("@", "")

        self.state = _DeviceState.RESERVED
        self.reserved_for = target_user_id
        self.reserved_by = user_id
        self.reserved_at = datetime.now().isoformat()

        return OperationResult.ok(
            f"Устройство {self.name} зарезервировано для {target_name}",
            {"target_user": target_user_id, "reserved_by": user_name},
        )

    def cancel_reservation(self, user_id: str) -> OperationResult:
        """Cancel reservation (only by who made it)"""
        if self.state != _DeviceState.RESERVED:
            return OperationResult.error(f"Устройство {self.name} не зарезервировано")

        if self.reserved_by != user_id:
            return OperationResult.error(
                f"Вы не можете отменить резервирование устройства {self.name}"
            )

        self.state = _DeviceState.FREE
        self.reserved_for = None
        self.reserved_by = None
        self.reserved_at = None

        return OperationResult.ok(f"Резервирование устройства {self.name} отменено")

    def rename(self, new_name: str) -> OperationResult:
        """Rename device"""
        old_name = self.name
        self.name = new_name
        return OperationResult.ok(f"Устройство {old_name} переименовано в {new_name}")

    def is_reserved_by(self, user_id: str) -> bool:
        return self.reserved_by == user_id

    def is_reserved_for(self, user_id: str) -> bool:
        return self.reserved_for == user_id

    def get_reserve_info(self) -> Optional[str]:
        """Get reservation info for display"""
        if self.state != _DeviceState.RESERVED or not self.reserved_for:
            return None
        reserved_for_name = self.reserved_for.split(":")[0].replace("@", "")
        reserved_by_name = (
            self.reserved_by.split(":")[0].replace("@", "")
            if self.reserved_by
            else "unknown"
        )
        return f"зарезервировано для {reserved_for_name} (кем: {reserved_by_name})"

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
# Device Registry with subscription management
# ============================================================================


class DeviceRegistry:
    """Manages device storage, search and subscriptions"""

    def __init__(self, data_file: str):
        self.data_file = data_file
        self.devices: Dict[str, Device] = {}
        self._load()

    def _load(self):
        """Load devices from file"""
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, "r") as f:
                    data = json.load(f)
                    for name, info in data.items():
                        state_value = info.get("state", "free")
                        if state_value == "reserved":
                            state = _DeviceState.RESERVED
                        elif state_value == "occupied":
                            state = _DeviceState.OCCUPIED
                        elif state_value == "maintenance":
                            state = _DeviceState.MAINTENANCE
                        else:
                            state = _DeviceState.FREE

                        self.devices[name] = Device(
                            name=name,
                            state=state,
                            users=info.get("users", []),
                            occupied_at=info.get("occupied_at"),
                            room_id=info.get("room_id"),
                            reserved_for=info.get("reserved_for"),
                            reserved_by=info.get("reserved_by"),
                            reserved_at=info.get("reserved_at"),
                            subscriptions=info.get("subscriptions", []),
                            subscription_rooms=info.get("subscription_rooms", {}),
                        )
                logger.info(f"Загружено устройств: {len(self.devices)}")
            except Exception as e:
                logger.error(f"Ошибка загрузки устройств: {e}")
                self.devices = {}

    def _save_devices(self):
        """Save devices to file"""
        data = {}
        for name, device in self.devices.items():
            if isinstance(device.state, _DeviceState):
                state_value = device.state.value
            else:
                state_value = device.state

            data[name] = {
                "state": state_value,
                "users": device.users,
                "occupied_at": device.occupied_at,
                "room_id": device.room_id,
                "reserved_for": device.reserved_for,
                "reserved_by": device.reserved_by,
                "reserved_at": device.reserved_at,
                "subscriptions": device.subscriptions,
                "subscription_rooms": device.subscription_rooms,
            }
        try:
            with open(self.data_file, "w") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.debug(f"Сохранено устройств: {len(self.devices)}")
        except Exception as e:
            logger.error(f"Ошибка сохранения устройств: {e}")

    def find_by_prefix(self, prefix: str) -> Optional[str]:
        """Find device by case-insensitive prefix"""
        prefix_lower = prefix.lower()
        matches = []

        for device_name in self.devices.keys():
            if device_name.lower().startswith(prefix_lower):
                matches.append(device_name)

        if len(matches) == 1:
            return matches[0]
        elif len(matches) > 1:
            return None
        return None

    def get(
        self, name_or_prefix: str
    ) -> Tuple[Optional[Device], Optional[str], Optional[str]]:
        """Get device by full name or prefix."""
        """Returns (device, device_name, error_message)"""
        if name_or_prefix in self.devices:
            return self.devices[name_or_prefix], name_or_prefix, None

        device_name = self.find_by_prefix(name_or_prefix)
        if device_name:
            return self.devices[device_name], device_name, None

        return None, None, f"Устройство {name_or_prefix} не найдено"

    def get_all(self) -> Dict[str, Device]:
        return self.devices

    def add(self, name: str) -> OperationResult:
        """Add a new device"""
        if name in self.devices:
            return OperationResult.error(f"Устройство {name} уже существует")

        self.devices[name] = Device(name=name)
        self._save_devices()
        return OperationResult.ok(f"Устройство {name} добавлено")

    def remove(self, name: str) -> OperationResult:
        """Remove a device"""
        if name not in self.devices:
            return OperationResult.error(f"Устройство {name} не найдено")

        del self.devices[name]
        self._save_devices()
        return OperationResult.ok(f"Устройство {name} удалено")

    def rename(self, old_name: str, new_name: str, user_id: str) -> OperationResult:
        """Rename a device"""
        device, device_name, error = self.get(old_name)
        if error:
            return OperationResult.error(error)

        if new_name in self.devices:
            return OperationResult.error(f"Устройство {new_name} уже существует")

        result = device.rename(new_name)
        if result.success:
            # Update device key in dictionary
            self.devices[new_name] = self.devices.pop(old_name)
            self._save_devices()

        return result

    def occupy(
        self, name: str, user_id: str, room_id: str
    ) -> Tuple[OperationResult, Optional[Device]]:
        """Occupy a device"""
        device, device_name, error = self.get(name)
        if error:
            return OperationResult.error(error), None

        result = device.occupy(user_id, room_id)
        if result.success:
            self._save_devices()
        return result, device

    def free(self, name: str, user_id: str) -> Tuple[OperationResult, Optional[Device]]:
        """Free a device"""
        device, device_name, error = self.get(name)
        if error:
            return OperationResult.error(error), None

        result = device.free(user_id)
        if result.success:
            self._save_devices()
        return result, device

    def reserve(
        self, name: str, user_id: str, target_user_id: str
    ) -> Tuple[OperationResult, Optional[Device]]:
        """Reserve a device"""
        device, device_name, error = self.get(name)
        if error:
            return OperationResult.error(error), None

        result = device.reserve(user_id, target_user_id)
        if result.success:
            self._save_devices()
        return result, device

    def cancel_reservation(
        self, name: str, user_id: str
    ) -> Tuple[OperationResult, Optional[Device]]:
        """Cancel reservation"""
        device, device_name, error = self.get(name)
        if error:
            return OperationResult.error(error), None

        result = device.cancel_reservation(user_id)
        if result.success:
            self._save_devices()
        return result, device

    def kick(
        self, name: str, admin_id: str, target_user_id: str
    ) -> Tuple[OperationResult, Optional[Device]]:
        """Kick user from device"""
        device, device_name, error = self.get(name)
        if error:
            return OperationResult.error(error), None

        result = device.kick_user(admin_id, target_user_id)
        if result.success:
            self._save_devices()
        return result, device

    def kick_all(self, admin_id: str) -> OperationResult:
        """Free all occupied devices"""
        kicked_users = []
        admin_name = admin_id.split(":")[0].replace("@", "")

        for device_name, device in self.devices.items():
            if device.state == _DeviceState.OCCUPIED:
                for kicked_user in device.users:
                    kicked_users.append((kicked_user, device_name, device.room_id))
                device.users = []
                if device.reserved_for:
                    device.state = _DeviceState.RESERVED
                else:
                    device.state = _DeviceState.FREE
                device.occupied_at = None
                device.room_id = None

        if kicked_users:
            self._save_devices()
            return OperationResult.ok(
                f"Kickall выполнен! Освобождено устройств: {len(kicked_users)}",
                {"kicked_users": kicked_users, "admin_name": admin_name},
            )
        else:
            return OperationResult.error("Нет занятых устройств для освобождения")

    # Subscription methods
    def subscribe(self, device_name: str, user_id: str, room_id: str) -> bool:
        """Subscribe user to device notifications"""
        device, _, error = self.get(device_name)
        if error:
            return False

        if user_id in device.subscriptions:
            return False

        device.subscriptions.append(user_id)
        device.subscription_rooms[user_id] = room_id

        self._save_devices()
        return True

    def unsubscribe(self, device_name: str, user_id: str) -> bool:
        """Unsubscribe user from device"""
        device, _, error = self.get(device_name)
        if error:
            return False

        if user_id not in device.subscriptions:
            return False

        device.subscriptions.remove(user_id)
        if user_id in device.subscription_rooms:
            del device.subscription_rooms[user_id]

        self._save_devices()
        return True

    def get_subscribers(self, device_name: str) -> List[str]:
        """Get list of subscribers for a device"""
        device, _, error = self.get(device_name)
        if error:
            return []
        return device.subscriptions.copy()

    def get_user_subscriptions(self, user_id: str) -> List[str]:
        """Get devices user is subscribed to"""
        subscribed = []
        for device_name, device in self.devices.items():
            if user_id in device.subscriptions:
                subscribed.append(device_name)
        return subscribed

    def get_subscription_room(self, user_id: str, device_name: str) -> Optional[str]:
        """Get room ID for a user's subscription to a device"""
        device, _, error = self.get(device_name)
        if error:
            return None
        return device.subscription_rooms.get(user_id)

    async def notify_subscribers(
        self, connection, device_name: str, freed_by: Optional[str] = None
    ):
        """Notify all subscribers about device availability"""
        device, _, error = self.get(device_name)
        if error or not device.subscriptions:
            return

        freed_by_name = (
            freed_by.split(":")[0].replace("@", "") if freed_by else "кем-то"
        )

        for subscriber in device.subscriptions.copy():
            room_id = device.subscription_rooms.get(subscriber)
            if room_id:
                message = (f"🔔 Устройство {device_name} "
                    f"освободилось!\n\nОсвободил: {freed_by_name}")
                await connection.send_message(room_id, message)
                self.unsubscribe(device_name, subscriber)


# ============================================================================
# Matrix Connection Manager
# ============================================================================


class MatrixConnection:
    """Manages Matrix connection and message sending"""

    def __init__(self, homeserver: str, username: str, password: str):
        self.homeserver = homeserver
        self.username = username
        self.password = password
        self.client: Optional[AsyncClient] = None
        self.user_id: Optional[str] = None
        self.user_rooms: Dict[str, str] = {}
        self.next_batch: Optional[str] = None

    async def connect(self) -> bool:
        """Connect and login to Matrix server"""
        logger.info(f"Запуск бота {self.username} на сервере {self.homeserver}")

        self.client = AsyncClient(self.homeserver, self.username)

        try:
            response = await self.client.login(self.password)
        except Exception as e:
            logger.error(f"Ошибка подключения: {e}")
            return False

        if isinstance(response, LoginResponse):
            logger.info(f"Бот успешно залогинен как {self.username}")
            logger.info(f"User ID: {self.client.user_id}")
            self.user_id = self.client.user_id
            self._load_batch_token()
            return True
        else:
            logger.error(f"Ошибка логина: {response}")
            return False

    async def disconnect(self):
        """Disconnect from Matrix server"""
        if self.client:
            await self.client.close()
            logger.info("Соединение закрыто")

    async def send_message(self, room_id: str, message: str):
        """Send a simple message"""
        try:
            await self.client.room_send(
                room_id=room_id,
                message_type="m.room.message",
                content={"msgtype": "m.text", "body": message},
            )
        except Exception as e:
            logger.error(f"Ошибка отправки сообщения: {e}")

    async def send_notification(self, user_id: str, message: str):
        """Send notification to user"""
        try:
            if user_id in self.user_rooms:
                await self.send_message(self.user_rooms[user_id], message)
                return

            response = await self.client.room_create(
                invite=[user_id],
                visibility="private",
                name="Уведомления от бота устройств",
            )

            if hasattr(response, "room_id"):
                room_id = response.room_id
                self.user_rooms[user_id] = room_id
                await self.send_message(
                    room_id, "👋 Привет! Я бот управления устройствами."
                )
                await self.send_message(room_id, message)
                logger.info(f"Создана комната уведомлений для {user_id}")
            else:
                logger.error(f"Не удалось создать комнату для {user_id}")
        except Exception as e:
            logger.error(f"Ошибка уведомления: {e}")

    async def join_room(self, room_id: str):
        """Join a room"""
        try:
            await self.client.join(room_id)
            logger.info(f"Бот присоединился к комнате {room_id}")
        except Exception as e:
            logger.error(f"Ошибка присоединения к комнате: {e}")

    async def sync(self, timeout: int) -> Any:
        """Sync with Matrix server"""
        return await self.client.sync(timeout=timeout, since=self.next_batch)

    def update_sync_token(self, token: str):
        """Update sync token"""
        self.next_batch = token
        self._save_batch_token()

    def _load_batch_token(self):
        """Load sync token from file"""
        if os.path.exists(SYNC_TOKEN_FILE):
            try:
                with open(SYNC_TOKEN_FILE, "r") as f:
                    self.next_batch = f.read().strip()
            except Exception as e:
                logger.error(f"Ошибка загрузки токена синхронизации: {e}")

    def _save_batch_token(self):
        """Save sync token to file"""
        if self.next_batch:
            try:
                with open(SYNC_TOKEN_FILE, "w") as f:
                    f.write(self.next_batch)
            except Exception as e:
                logger.error(f"Ошибка сохранения токена синхронизации: {e}")


# ============================================================================
# Command Classes
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
        admin_only: bool = False,
    ):
        self.names = names
        self.description = description
        self.handler = handler
        self.min_args = min_args
        self.admin_only = admin_only

    def matches(self, input_name: str) -> bool:
        input_lower = input_name.lower()
        for name in self.names:
            if name == input_lower or (
                len(input_lower) >= 1 and name.startswith(input_lower)
            ):
                return True
        return False

    async def execute(self, bot: "DeviceBot", context: CommandContext) -> None:
        if len(context.args) < self.min_args:
            await bot.connection.send_message(
                context.room_id,
                (f"❌ Недостаточно аргументов. Использование: "
                    f"!{self.names[0]} ...\n{self.description}"),
            )
            return
        await self.handler(bot, context)


class CommandRegistry:
    """Registry for managing commands"""

    def __init__(self):
        self.commands: List[Command] = []
        self._command_map: Dict[str, Command] = {}

    def register(self, command: Command) -> None:
        self.commands.append(command)
        for name in command.names:
            self._command_map[name] = command

    def resolve_command(self, input_name: str) -> Optional[Tuple[Command, str]]:
        input_lower = input_name.lower()

        if input_lower in self._command_map:
            return (self._command_map[input_lower], input_lower)

        matches = []
        for cmd in self.commands:
            for name in cmd.names:
                if name.startswith(input_lower):
                    matches.append((cmd, name))
                    break

        if len(matches) == 1:
            return matches[0]
        return None


# ============================================================================
# Main Bot Class
# ============================================================================


class DeviceBot:
    """Main bot class orchestrating all components"""

    def __init__(self):
        self.connection: Optional[MatrixConnection] = None
        self.registry: Optional[DeviceRegistry] = None
        self.command_registry: CommandRegistry = CommandRegistry()
        self.state: _BotState = _BotState.STOPPED

        self._load_data()
        self._register_commands()

    def _load_data(self):
        """Load all persistent data"""
        self.registry = DeviceRegistry(DATA_FILE)
        self.connection = MatrixConnection(HOMESERVER, USERNAME, PASSWORD)

    def _register_commands(self):
        """Register all commands with their handlers"""

        # list command
        async def handle_list(bot: DeviceBot, ctx: CommandContext):
            await bot._send_device_list(ctx.room_id)

        self.command_registry.register(
            Command(
                names=["list", "l", "ls"],
                description="Показать все устройства",
                handler=handle_list,
            )
        )

        # get command
        async def handle_get(bot: DeviceBot, ctx: CommandContext):
            device_name_or_prefix = " ".join(ctx.args)
            user_name = ctx.sender.split(":")[0].replace("@", "")

            result, device = bot.registry.occupy(
                device_name_or_prefix, ctx.sender, ctx.room_id
            )

            if result.success:
                logger.info(
                    (
                        f"✅ ДЕЙСТВИЕ: {user_name} занял устройство "
                        f"'{device.name if device else '?'}'"
                    )
                )
                await bot.connection.send_message(ctx.room_id, f"✅ {result.message}")
                await bot._send_device_controls(
                    ctx.room_id,
                    device.name if device else device_name_or_prefix,
                    ctx.sender,
                )
            else:
                logger.info(
                    (
                        f"❌ ДЕЙСТВИЕ: {user_name} не смог занять "
                        f"'{device_name_or_prefix}' - {result.message}"
                    )
                )
                await bot.connection.send_message(ctx.room_id, f"❌ {result.message}")

        self.command_registry.register(
            Command(
                names=["get", "g"],
                description="Занять устройство. Использование: !g <устройство>",
                handler=handle_get,
                min_args=1,
            )
        )

        # free command
        async def handle_free(bot: DeviceBot, ctx: CommandContext):
            device_name_or_prefix = " ".join(ctx.args)
            user_name = ctx.sender.split(":")[0].replace("@", "")

            result, device = bot.registry.free(device_name_or_prefix, ctx.sender)

            if result.success:
                logger.info(
                    (
                        f"✅ ДЕЙСТВИЕ: {user_name} "
                        f"освободил устройство '{device.name if device else '?'}'"
                    )
                )
                await bot.connection.send_message(ctx.room_id, f"✅ {result.message}")
                await bot.registry.notify_subscribers(
                    bot.connection,
                    device.name if device else device_name_or_prefix,
                    ctx.sender,
                )
                await bot._send_device_list(ctx.room_id)
            else:
                logger.info(
                    (
                        f"❌ ДЕЙСТВИЕ: {user_name} не смог "
                        f"освободить '{device_name_or_prefix}' - {result.message}"
                    )
                )
                await bot.connection.send_message(ctx.room_id, f"❌ {result.message}")

        self.command_registry.register(
            Command(
                names=["free", "f"],
                description="Освободить устройство. Использование: !free <устройство>",
                handler=handle_free,
                min_args=1,
            )
        )

        # mydevices command
        async def handle_mydevices(bot: DeviceBot, ctx: CommandContext):
            user_devices = []
            for name, device in bot.registry.get_all().items():
                if device.is_occupied_by(ctx.sender):
                    user_devices.append((name, device))

            if user_devices:
                text = "📱 Ваши устройства:\n\n"
                for name, device in user_devices:
                    occupied_time = device.get_occupied_time()
                    time_str = f" (занято {occupied_time})" if occupied_time else ""
                    text += f"- {name}{time_str} — !free {name}\n"
                await bot.connection.send_message(ctx.room_id, text)
            else:
                await bot.connection.send_message(
                    ctx.room_id, "📭 Вы не занимаете ни одного устройства"
                )

        self.command_registry.register(
            Command(
                names=["mydevices", "m"],
                description="Показать мои устройства",
                handler=handle_mydevices,
            )
        )

        # subscribe command
        async def handle_subscribe(bot: DeviceBot, ctx: CommandContext):
            device_name_or_prefix = " ".join(ctx.args)
            device, device_name, error = bot.registry.get(device_name_or_prefix)

            if error:
                await bot.connection.send_message(ctx.room_id, f"❌ {error}")
                return

            user_name = ctx.sender.split(":")[0].replace("@", "")

            if bot.registry.subscribe(device_name, ctx.sender, ctx.room_id):
                logger.info(
                    f"📋 ДЕЙСТВИЕ: {user_name} подписался на уведомления '{device_name}'"
                )
                await bot.connection.send_message(
                    ctx.room_id,
                    f"✅ Вы подписались на уведомления об освобождении {device_name}",
                )
            else:
                logger.info(f"📋 ДЕЙСТВИЕ: {user_name} уже подписан на '{device_name}'")
                await bot.connection.send_message(
                    ctx.room_id, f"❌ Вы уже подписаны на {device_name}"
                )

        self.command_registry.register(
            Command(
                names=["subscribe", "s"],
                description=(
                    "Подписаться на уведомления. " "Использование: !s <устройство>"
                ),
                handler=handle_subscribe,
                min_args=1,
            )
        )

        # unsubscribe command
        async def handle_unsubscribe(bot: DeviceBot, ctx: CommandContext):
            device_name_or_prefix = " ".join(ctx.args)
            device, device_name, error = bot.registry.get(device_name_or_prefix)

            if error:
                await bot.connection.send_message(ctx.room_id, f"❌ {error}")
                return

            user_name = ctx.sender.split(":")[0].replace("@", "")

            if bot.registry.unsubscribe(device_name, ctx.sender):
                logger.info(
                    f"📋 ДЕЙСТВИЕ: {user_name} отписался от уведомлений '{device_name}'"
                )
                await bot.connection.send_message(
                    ctx.room_id, f"✅ Вы отписались от уведомлений {device_name}"
                )
            else:
                await bot.connection.send_message(
                    ctx.room_id, f"❌ Вы не были подписаны на {device_name}"
                )

        self.command_registry.register(
            Command(
                names=["unsubscribe", "u"],
                description="Отписаться от уведомлений. Использование: !u <устройство>",
                handler=handle_unsubscribe,
                min_args=1,
            )
        )

        # subscriptions command
        async def handle_subscriptions(bot: DeviceBot, ctx: CommandContext):
            subscribed = bot.registry.get_user_subscriptions(ctx.sender)
            if subscribed:
                text = "📋 Ваши подписки:\n\n"
                for device in subscribed:
                    text += f"- {device}\n"
                await bot.connection.send_message(ctx.room_id, text)
            else:
                await bot.connection.send_message(
                    ctx.room_id, "📭 Вы не подписаны ни на одно устройство"
                )

        self.command_registry.register(
            Command(
                names=["subscriptions"],
                description="Показать мои подписки",
                handler=handle_subscriptions,
            )
        )

        # subscribers command
        async def handle_subscribers(bot: DeviceBot, ctx: CommandContext):
            device_name_or_prefix = " ".join(ctx.args)
            device, device_name, error = bot.registry.get(device_name_or_prefix)

            if error:
                await bot.connection.send_message(ctx.room_id, f"❌ {error}")
                return

            subscribers = bot.registry.get_subscribers(device_name)
            if subscribers:
                subscriber_names = [
                    s.split(":")[0].replace("@", "") for s in subscribers
                ]
                text = f"👥 Подписчики устройства {device_name}:\n\n"
                for name in subscriber_names:
                    text += f"- {name}\n"
                await bot.connection.send_message(ctx.room_id, text)
            else:
                await bot.connection.send_message(
                    ctx.room_id, f"📭 На устройство {device_name} никто не подписан"
                )

        self.command_registry.register(
            Command(
                names=["subscribers", "ss"],
                description=(
                    "Показать подписчиков устройства. "
                    "Использование: !ss <устройство>"
                ),
                handler=handle_subscribers,
                min_args=1,
            )
        )

        # reserve command
        async def handle_reserve(bot: DeviceBot, ctx: CommandContext):
            device_name_or_prefix = ctx.args[0]
            target_user = ctx.args[1]

            if ":" in target_user:
                target_user_id = target_user
            else:
                target_user_id = f"@{target_user}:{LOCALPART_DOMAIN}"

            user_name = ctx.sender.split(":")[0].replace("@", "")
            target_name = target_user_id.split(":")[0].replace("@", "")

            result, device = bot.registry.reserve(
                device_name_or_prefix, ctx.sender, target_user_id
            )

            if result.success:
                logger.info(
                    (
                        f"✅ ДЕЙСТВИЕ: {user_name} зарезервировал "
                        f"'{device.name if device else '?'}' для {target_name}"
                    )
                )
                await bot.connection.send_message(ctx.room_id, f"✅ {result.message}")

                if result.data:
                    await bot.connection.send_notification(
                        target_user_id,
                        (
                            f"🔔 Пользователь {result.data['reserved_by']} "
                            f"зарезервировал для вас устройство "
                            f"{device.name if device else device_name_or_prefix}\n"
                            f"\nВы можете занять его командой !g "
                            f"{device.name if device else device_name_or_prefix}"
                        ),
                    )

                await bot._send_device_list(ctx.room_id)
            else:
                logger.info(
                    (
                        f"❌ ДЕЙСТВИЕ: {user_name} не смог зарезервировать "
                        f"'{device_name_or_prefix}' - {result.message}"
                    )
                )
                await bot.connection.send_message(ctx.room_id, f"❌ {result.message}")

        self.command_registry.register(
            Command(
                names=["reserve", "res"],
                description=(
                    "Зарезервировать устройство для пользователя. "
                    "Использование: !reserve <устройство> <пользователь>"
                ),
                handler=handle_reserve,
                min_args=2,
            )
        )

        # cancel command
        async def handle_cancel(bot: DeviceBot, ctx: CommandContext):
            device_name_or_prefix = " ".join(ctx.args)
            user_name = ctx.sender.split(":")[0].replace("@", "")

            result, device = bot.registry.cancel_reservation(
                device_name_or_prefix, ctx.sender
            )

            if result.success:
                logger.info(
                    (
                        f"✅ ДЕЙСТВИЕ: {user_name} отменил резервирование "
                        f"'{device.name if device else '?'}'"
                    )
                )
                await bot.connection.send_message(ctx.room_id, f"✅ {result.message}")
                await bot._send_device_list(ctx.room_id)
            else:
                logger.info(
                    (
                        f"❌ ДЕЙСТВИЕ: {user_name} не смог отменить "
                        f"резервирование - {result.message}"
                    )
                )
                await bot.connection.send_message(ctx.room_id, f"❌ {result.message}")

        self.command_registry.register(
            Command(
                names=["cancel", "c"],
                description=(
                    "Отменить резервирование. " "Использование: !cancel <устройство>"
                ),
                handler=handle_cancel,
                min_args=1,
            )
        )

        # rename command
        async def handle_rename(bot: DeviceBot, ctx: CommandContext):
            if len(ctx.args) < 2:
                await bot.connection.send_message(
                    ctx.room_id, "❌ Использование: !rename <старое_имя> <новое_имя>"
                )
                return

            old_name = ctx.args[0]
            new_name = ctx.args[1]
            user_name = ctx.sender.split(":")[0].replace("@", "")

            result = bot.registry.rename(old_name, new_name, ctx.sender)

            if result.success:
                logger.info(
                    (
                        f"✅ АДМИН: {user_name} переименовал устройство "
                        f"'{old_name}' в '{new_name}'"
                    )
                )
                await bot.connection.send_message(ctx.room_id, f"✅ {result.message}")
                await bot._send_device_list(ctx.room_id)
            else:
                logger.info(
                    f"❌ АДМИН: {user_name} не смог переименовать - {result.message}"
                )
                await bot.connection.send_message(ctx.room_id, f"❌ {result.message}")

        self.command_registry.register(
            Command(
                names=["rename", "move", "mv"],
                description=(
                    "Переименовать устройство. Использование: "
                    "!rename <старое> <новое>"
                ),
                handler=handle_rename,
                min_args=2,
                admin_only=True,
            )
        )

        # add command
        async def handle_add(bot: DeviceBot, ctx: CommandContext):
            device_name = " ".join(ctx.args)
            user_name = ctx.sender.split(":")[0].replace("@", "")

            result = bot.registry.add(device_name)

            if result.success:
                logger.info(f"✅ АДМИН: {user_name} добавил устройство '{device_name}'")
                await bot.connection.send_message(ctx.room_id, f"✅ {result.message}")
                await bot._send_device_list(ctx.room_id)
            else:
                logger.info(
                    (
                        f"❌ АДМИН: {user_name} не смог добавить "
                        f"'{device_name}' - {result.message}"
                    )
                )
                await bot.connection.send_message(ctx.room_id, f"❌ {result.message}")

        self.command_registry.register(
            Command(
                names=["add", "a", "new", "n"],
                description="Добавить устройство. Использование: !a <устройство>",
                handler=handle_add,
                min_args=1,
                admin_only=True,
            )
        )

        # remove command
        async def handle_remove(bot: DeviceBot, ctx: CommandContext):
            device_name = " ".join(ctx.args)
            user_name = ctx.sender.split(":")[0].replace("@", "")

            result = bot.registry.remove(device_name)

            if result.success:
                logger.info(f"✅ АДМИН: {user_name} удалил устройство '{device_name}'")
                await bot.connection.send_message(ctx.room_id, f"✅ {result.message}")
                await bot._send_device_list(ctx.room_id)
            else:
                logger.info(
                    (
                        f"❌ АДМИН: {user_name} не смог удалить "
                        f"'{device_name}' - {result.message}"
                    )
                )
                await bot.connection.send_message(ctx.room_id, f"❌ {result.message}")

        self.command_registry.register(
            Command(
                names=["remove", "r", "rm"],
                description="Удалить устройство. Использование: !r <устройство>",
                handler=handle_remove,
                min_args=1,
                admin_only=True,
            )
        )

        # kick command
        async def handle_kick(bot: DeviceBot, ctx: CommandContext):
            device_name_or_prefix = ctx.args[0]
            target_user = ctx.args[1] if len(ctx.args) > 1 else None

            if target_user:
                if ":" in target_user:
                    target_user_id = target_user
                else:
                    target_user_id = f"@{target_user}:{LOCALPART_DOMAIN}"
            else:
                device, _, error = bot.registry.get(device_name_or_prefix)
                if error:
                    await bot.connection.send_message(ctx.room_id, f"❌ {error}")
                    return
                if device and device.users:
                    target_user_id = device.users[0]
                else:
                    await bot.connection.send_message(
                        ctx.room_id,
                        (
                            f"❌ Нет пользователя для кика "
                            f"с устройства {device_name_or_prefix}"
                        ),
                    )
                    return

            admin_name = ctx.sender.split(":")[0].replace("@", "")
            result, device = bot.registry.kick(
                device_name_or_prefix, ctx.sender, target_user_id
            )

            if result.success:
                logger.info(
                    (
                        f"✅ АДМИН: {admin_name} кикнул "
                        f"пользователя с устройства '{device.name if device else '?'}'"
                    )
                )
                await bot.connection.send_message(ctx.room_id, f"✅ {result.message}")

                if result.data and result.data.get("kicked_user"):
                    if result.data.get("room_id"):
                        await bot.connection.send_message(
                            result.data["room_id"],
                            (
                                f"⚠️ Вас кикнули с устройства "
                                f"{device.name if device else device_name_or_prefix}\n"
                                f"\nПо запросу пользователя {admin_name}"
                            ),
                        )
                    else:
                        await bot.connection.send_notification(
                            result.data["kicked_user"],
                            (
                                f"⚠️ Вас кикнули с устройства "
                                f"{device.name if device else device_name_or_prefix}\n"
                                f"\nПо запросу пользователя {admin_name}"
                            ),
                        )

                await bot.registry.notify_subscribers(
                    bot.connection,
                    device.name if device else device_name_or_prefix,
                    ctx.sender,
                )
                await bot._send_device_list(ctx.room_id)
            else:
                logger.info(
                    f"❌ АДМИН: {admin_name} не смог кикнуть - {result.message}"
                )
                await bot.connection.send_message(ctx.room_id, f"❌ {result.message}")

        self.command_registry.register(
            Command(
                names=["kick", "k"],
                description=(
                    "Кикнуть пользователя с устройства. "
                    "Использование: !k <устройство> [пользователь]"
                ),
                handler=handle_kick,
                min_args=1,
            )
        )

        # kickall command
        async def handle_kickall(bot: DeviceBot, ctx: CommandContext):
            admin_name = ctx.sender.split(":")[0].replace("@", "")

            result = bot.registry.kick_all(ctx.sender)

            if result.success:
                logger.info(
                    f"✅ АДМИН: {admin_name} выполнил kickall - {result.message}"
                )
                await bot.connection.send_message(ctx.room_id, f"✅ {result.message}")

                if result.data:
                    for kicked_user, device_name, room_id in result.data[
                        "kicked_users"
                    ]:
                        kicked_name = kicked_user.split(":")[0].replace("@", "")
                        if room_id:
                            await bot.connection.send_message(
                                room_id,
                                (
                                    f"⚠️ Вас кикнули с устройства "
                                    f"{device_name}\n\nПо запросу {admin_name} "
                                    "(команда kickall)"
                                ),
                            )
                        else:
                            await bot.connection.send_notification(
                                kicked_user,
                                (
                                    f"⚠️ Вас кикнули с устройства "
                                    f"{device_name}\n\nПо запросу "
                                    f"{admin_name} (команда kickall)"
                                ),
                            )

                for device_name in bot.registry.get_all():
                    await bot.registry.notify_subscribers(
                        bot.connection, device_name, ctx.sender
                    )

                await bot._send_device_list(ctx.room_id)
            else:
                await bot.connection.send_message(ctx.room_id, f"❌ {result.message}")

        self.command_registry.register(
            Command(
                names=["kickall", "ka"],
                description="Освободить все устройства",
                handler=handle_kickall,
                admin_only=True,
            )
        )

        # help command
        async def handle_help(bot: DeviceBot, ctx: CommandContext):
            help_text = "🤖 Команды бота:\n\n"

            for cmd in bot.command_registry.commands:
                help_text += f"!{cmd.names[0]}"
                if len(cmd.names) > 1:
                    aliases = [f"!{n}" for n in cmd.names[1:] if n != cmd.names[0]]
                    if aliases:
                        help_text += f" ({', '.join(aliases)})"
                help_text += f" — {cmd.description}\n"

            help_text += "\nДоступные устройства:\n\n"
            for name, device in bot.registry.get_all().items():
                if device.state == _DeviceState.FREE:
                    help_text += f"- {name} — ✅ свободно\n"
                elif device.state == _DeviceState.RESERVED:
                    reserve_info = device.get_reserve_info()
                    help_text += f"- {name} — 🟡 {reserve_info}\n"
                else:
                    occupied_time = device.get_occupied_time()
                    time_str = f" (занято {occupied_time})" if occupied_time else ""
                    help_text += (
                        f"- {name} — 🔴 занято: {', '.join(device.owners)}{time_str}\n"
                    )

            await bot.connection.send_message(ctx.room_id, help_text)

        self.command_registry.register(
            Command(
                names=["help", "h"], description="Показать справку", handler=handle_help
            )
        )

    # UI methods
    async def _send_device_list(self, room_id: str):
        """Send device list"""
        devices = self.registry.get_all()

        if not devices:
            text = (
                "📭 Список устройств пуст. Добавьте устройства командой !add <название>"
            )
        else:
            text = "📱 Список устройств:\n\n"
            for name, device in devices.items():
                if device.state == _DeviceState.FREE:
                    text += f"- {name} — ✅ Свободно\n"
                elif device.state == _DeviceState.RESERVED:
                    reserve_info = device.get_reserve_info()
                    text += f"- {name} — 🟡 {reserve_info}\n"
                elif device.state == _DeviceState.MAINTENANCE:
                    text += f"- {name} — 🔧 Обслуживание\n"
                else:
                    occupied_time = device.get_occupied_time()
                    time_str = f" (занято {occupied_time})" if occupied_time else ""
                    text += (
                        f"- {name} — 🔴 Занято: {', '.join(device.owners)}{time_str}\n"
                    )

            text += "\n💡 Как занять устройство:\n"
            available = [
                name for name, d in devices.items() if d.state == _DeviceState.FREE
            ]
            if available:
                text += "Скопируйте и отправьте команду:\n"
                for device in available[:3]:
                    text += f"  !g {device}\n"
                if len(available) > 3:
                    text += f"  и ещё {len(available) - 3} устройств\n"
            else:
                reserved = [
                    name
                    for name, d in devices.items()
                    if d.state == _DeviceState.RESERVED
                ]
                if reserved:
                    text += "❌ Нет свободных устройств, но некоторые зарезервированы\n"
                    text += (
                        "💡 Чтобы занять зарезервированное устройство, "
                        "нужно быть тем, для кого оно зарезервировано\n"
                    )
                else:
                    text += "❌ Нет свободных устройств\n"

            text += (
                "\nПодсказка: Можно писать только начало имени "
                "устройства, например !g esp\n"
            )
            text += "\nДругие команды:\n"
            text += "!mydevices — мои устройства\n"
            text += "!help — все команды"

        await self.connection.send_message(room_id, text)

    async def _send_device_controls(self, room_id: str, device_name: str, user_id: str):
        """Send device controls"""
        device = self.registry.devices.get(device_name)
        if not device:
            return

        is_owner = device.is_occupied_by(user_id)
        status = "🔴 Занято" if device.state == _DeviceState.OCCUPIED else "✅ Свободно"

        text = f"🖥️ {device_name}\n"
        text += f"Статус: {status}\n"
        text += f"Владельцы: {', '.join(device.owners) if device.owners else 'никем'}\n"

        occupied_time = device.get_occupied_time()
        if occupied_time:
            text += f"Время занятия: {occupied_time}\n"

        if device.state == _DeviceState.RESERVED:
            reserve_info = device.get_reserve_info()
            text += f"Резерв: {reserve_info}\n"

        text += "\n"

        if is_owner:
            text += f"🔓 Чтобы освободить: !free {device_name}\n\n"

        text += "Другие команды:\n"
        text += "!list — список устройств\n"
        text += "!mydevices — мои устройства"

        await self.connection.send_message(room_id, text)

    async def _handle_command(self, room_id: str, sender: str, command: str):
        """Handle commands with ambiguous resolution"""
        parts = command.strip().split()
        if not parts:
            return

        cmd_name = parts[0].lower()
        result = self.command_registry.resolve_command(cmd_name)

        if result:
            command_obj, matched_name = result
            # Parse arguments respecting quotes
            args = []
            current_arg = ""
            in_quotes = False
            for part in parts[1:]:
                if part.startswith('"') and not in_quotes:
                    in_quotes = True
                    current_arg = part[1:]
                elif part.endswith('"') and in_quotes:
                    in_quotes = False
                    current_arg += " " + part[:-1] if current_arg else part[:-1]
                    args.append(current_arg)
                    current_arg = ""
                elif in_quotes:
                    current_arg += " " + part
                else:
                    args.append(part)

            if current_arg:
                args.append(current_arg)

            context = CommandContext(
                room_id=room_id, sender=sender, args=args, raw_command=command
            )

            await command_obj.execute(self, context)
        else:
            user_name = sender.split(":")[0].replace("@", "")
            logger.info(
                f"❌ ДЕЙСТВИЕ: {user_name} ввел неизвестную команду '{cmd_name}'"
            )
            await self.connection.send_message(
                room_id,
                f"❌ Неизвестная команда: {cmd_name}\nВведите !help для списка команд",
            )

    async def run(self):
        """Run the bot"""
        if not await self.connection.connect():
            self.state = _BotState.ERROR
            return

        self.state = _BotState.RUNNING
        logger.info("🤖 Бот запущен и готов к работе!")
        logger.info("💡 Пригласите бота в комнату и напишите !help")

        try:
            while self.state == _BotState.RUNNING:
                try:
                    sync_response = await self.connection.sync(SYNC_TIMEOUT)

                    self.connection.update_sync_token(sync_response.next_batch)

                    # Handle invites
                    if hasattr(sync_response, "rooms") and hasattr(
                        sync_response.rooms, "invite"
                    ):
                        for room_id in sync_response.rooms.invite:
                            await self.connection.join_room(room_id)

                    # Handle messages
                    if hasattr(sync_response, "rooms") and hasattr(
                        sync_response.rooms, "join"
                    ):
                        for room_id, room_data in sync_response.rooms.join.items():
                            for event in room_data.timeline.events:
                                if (
                                    isinstance(event, RoomMessageText)
                                    and event.sender != self.connection.user_id
                                ):
                                    body = event.body.strip()
                                    if body.startswith(COMMAND_PREFIX):
                                        command = body[len(COMMAND_PREFIX) :]
                                        await self._handle_command(
                                            room_id, event.sender, command
                                        )

                except Exception as e:
                    logger.error(f"Ошибка синхронизации: {e}")
                    await asyncio.sleep(5)

        except KeyboardInterrupt:
            logger.info("\n🛑 Бот остановлен по команде пользователя")
            self.state = _BotState.STOPPED
        except Exception as e:
            logger.error(f"❌ Критическая ошибка: {e}")
        finally:
            await self.connection.disconnect()


if __name__ == "__main__":
    bot = DeviceBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("Программа завершена")
