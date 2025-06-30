from db import InMemoryDB


class InMemoryDBApp:
    def __init__(self):
        self.db = InMemoryDB()
        self.running = True

    def execute(self, line):
        if line is None:
            self.running = False
            return None
        line = line.strip()
        if not line:
            return None
        parts = line.split()
        cmd = parts[0].upper()
        args = parts[1:]

        if cmd == "END":
            self.running = False
            return None
        elif cmd == "SET" and len(args) == 2:
            self.db.set(args[0], args[1])
            return None
        elif cmd == "GET" and len(args) == 1:
            value = self.db.get(args[0])
            return value if value is not None else "NULL"
        elif cmd == "UNSET" and len(args) == 1:
            self.db.unset(args[0])
            return None
        elif cmd == "COUNTS" and len(args) == 1:
            return str(self.db.counts(args[0]))
        elif cmd == "FIND" and len(args) == 1:
            keys = self.db.find(args[0])
            return " ".join(keys) if keys else "NULL"
        elif cmd == "BEGIN" and len(args) == 0:
            self.db.begin()
            return None
        elif cmd == "ROLLBACK" and len(args) == 0:
            if not self.db.rollback():
                return "NO TRANSACTION"
            return None
        elif cmd == "COMMIT" and len(args) == 0:
            if not self.db.commit():
                return "NO TRANSACTION"
            return None
        else:
            return "UNKNOWN COMMAND"

    def run(self):
        while self.running:
            try:
                line = input()
            except EOFError:
                self.execute(None)
                break
            result = self.execute(line)
            if result is not None:
                print(result)


if __name__ == "__main__":
    app = InMemoryDBApp()
    app.run()
