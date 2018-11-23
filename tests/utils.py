class DummyDataset:
    def __init__(self, doc, name):
        self._name = name
        self._doc = doc

    def get_graph(self, name):
        return self._doc

    def has_graph(self, name):
        return self._name == name
