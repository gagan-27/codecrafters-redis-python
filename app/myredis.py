class MyRedis:
    def __init__(self):
        self.db = {}
    def set_value(self, key, value):
        self.db[key] = value
        print(self.db)
    def get_value(self, key):
        print(self.db)

        return self.db.get(key)