import copy


class InMemoryDB:
    def __init__(self):
        self.db = {}  # основная база
        self.value_to_keys = {}  # значение: set(ключей)
        self.transactions = []  # стек транзакций, каждый элемент: (db, value_to_keys)

    def _current_db(self):
        if self.transactions:
            return self.transactions[-1][0]
        return self.db

    def _current_value_to_keys(self):
        if self.transactions:
            return self.transactions[-1][1]
        return self.value_to_keys

    def set(self, key, value):
        db = self._current_db()
        value_to_keys = self._current_value_to_keys()
        if key in db:
            old_value = db[key]
            value_to_keys[old_value].discard(key)
            if not value_to_keys[old_value]:
                del value_to_keys[old_value]
        db[key] = value
        if value not in value_to_keys:
            value_to_keys[value] = set()
        value_to_keys[value].add(key)

    def get(self, key):
        db = self._current_db()
        return db.get(key)

    def unset(self, key):
        db = self._current_db()
        value_to_keys = self._current_value_to_keys()
        if key in db:
            value = db[key]
            value_to_keys[value].discard(key)
            if not value_to_keys[value]:
                del value_to_keys[value]
            del db[key]

    def counts(self, value):
        value_to_keys = self._current_value_to_keys()
        return len(value_to_keys.get(value, set()))

    def find(self, value):
        value_to_keys = self._current_value_to_keys()
        return sorted(value_to_keys.get(value, set()))

    def begin(self):
        # Копируем текущее состояние
        self.transactions.append(
            (
                copy.deepcopy(self._current_db()),
                copy.deepcopy(self._current_value_to_keys()),
            )
        )

    def rollback(self):
        if not self.transactions:
            return False
        self.transactions.pop()
        return True

    def commit(self):
        if not self.transactions:
            return False
        db, value_to_keys = self.transactions.pop()
        if self.transactions:
            # Сливаем в предыдущую транзакцию
            self.transactions[-1] = (db, value_to_keys)
        else:
            # Сливаем в основную базу
            self.db = db
            self.value_to_keys = value_to_keys
        return True
