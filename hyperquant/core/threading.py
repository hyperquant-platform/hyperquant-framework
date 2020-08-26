from threading import Condition, Lock


class Trigger:
    def __init__(self):
        self._cond = Condition(Lock())
        self._flag = False

    def call(self):
        with self._cond:
            self._flag = True
            self._cond.notify_all()

    def wait(self, timeout=None):
        with self._cond:
            signaled = self._flag
            if not signaled:
                signaled = self._cond.wait(timeout)
            self._flag = False
            return signaled
