
import time
from plyer import notification


notification.notify(
    title="🔔 Сделайте перерыв!",
    message="Работаете уже час — пора размяться.",
    timeout=7
)
