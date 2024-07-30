class DataSourceFactory:
    @staticmethod
    def get_data_source(source_type):
        if source_type == 'ActualLoadProd':
            return ActualLoadProd()
        elif source_type == 'api2':
            return API2()
        # Add more sources as needed
        else:
            raise ValueError("Unknown data source type")
if __name__ == '__main__':
    pass