from blinker import signal

setting_changed = signal("settings:changed")
project_opened = signal("project:opened")
project_closed = signal("project:closed")
