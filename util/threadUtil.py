import threading


class ThreadUtil:
    @classmethod
    def newThreadToInvoke(cls, job, args=()):
        threading.Thread(target=job, args=args).start()
