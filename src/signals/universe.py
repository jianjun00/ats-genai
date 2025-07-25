from copy import deepcopy
from datetime import date

class Universe:
    def __init__(self, current_date=None, instrument_ids=None):
        self.current_date = current_date
        # Accept list, set, or None; deduplicate while preserving order
        if instrument_ids is None:
            self.instrument_ids = []
        else:
            seen = set()
            self.instrument_ids = []
            for i in instrument_ids:
                if i not in seen:
                    seen.add(i)
                    self.instrument_ids.append(i)

    def advanceTo(self, new_date, new_instruments=None):
        self.current_date = new_date
        if new_instruments is not None:
            seen = set()
            self.instrument_ids = []
            for i in new_instruments:
                if i not in seen:
                    seen.add(i)
                    self.instrument_ids.append(i)

    def update_date(self, new_date, new_instruments=None):
        self.advanceTo(new_date, new_instruments)

    def add_instrument(self, instrument_id):
        if instrument_id not in self.instrument_ids:
            self.instrument_ids.append(instrument_id)

    def remove_instrument(self, instrument_id):
        if instrument_id in self.instrument_ids:
            self.instrument_ids.remove(instrument_id)

    def has_instrument(self, instrument_id):
        return instrument_id in self.instrument_ids

    def get_instrument_count(self):
        return len(self.instrument_ids)

    def copy(self):
        # Deep copy instrument_ids, but not self
        return Universe(current_date=self.current_date, instrument_ids=list(self.instrument_ids))

    def __len__(self):
        return len(self.instrument_ids)

    def __contains__(self, item):
        return item in self.instrument_ids

    def __iter__(self):
        return iter(self.instrument_ids)
