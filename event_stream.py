from __future__ import annotations
from pandas.core.frame import DataFrame


class EventStream:
    def __init__(self, dataframe: DataFrame, source_data_scheme: tuple, final_data_scheme: set):
        if any(statement not in source_data_scheme for statement in ('event_timestamp', 'user_id', 'event_name')) and 'event_column' not in source_data_scheme or \
           any(statement not in final_data_scheme for statement in {'event_timestamp', 'user_id', 'event_name'}):
            raise ValueError("expected at least these values: event_timestamp, user_id, event_name OR event_column")
        else:
            self.dataframe = dataframe
            self.source_data_scheme = source_data_scheme
            self.final_data_scheme = final_data_scheme
            self._init_dataframe()

    def __str__(self):
        return f"{self.source_data_scheme} to {self.final_data_scheme};\n{self.dataframe.describe()}"

    def _init_dataframe(self):
        self.dataframe['deleted'] = False

    def mapping(self, mapper: dict) -> ():
        """
        Modifies current dataframe according to the mapper
        :param mapper: dict with condition and result parameters
        :return: DataFrame (for convenience of user)
        """
        self.final_data_scheme |= mapper['result'].keys()

        for key in mapper['result']:
            self.dataframe[key] = None

        for index in self.dataframe.index:
            for result in mapper['result']:
                insertion_statement = mapper['result'][result]
                for condition in mapper['condition']:
                    if mapper['condition'][condition] not in self.dataframe.at[index, condition]:
                        insertion_statement = None
                        break
                if insertion_statement is not None:
                    self.dataframe.at[index, result] = insertion_statement
        return self.get_dataframe()

    def soft_delete(self, row_id: int | list) -> ():
        """
        Soft deleting from the dataframe by the row id (single and multiple rows supported)
        :param row_id:
        :return: DataFrame (for analysis convenience)
        """
        if type(row_id) is int:
            self.dataframe.at[row_id, 'deleted'] = True
        else:
            for index in row_id:
                self.dataframe.at[index, 'deleted'] = True
        return self.get_dataframe()

    def get_dataframe(self) -> DataFrame:
        return self.dataframe.loc[(self.dataframe['deleted'] == False), self.final_data_scheme]
