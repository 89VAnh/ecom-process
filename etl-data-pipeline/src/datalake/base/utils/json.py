class JsonUtils:
    @staticmethod
    def flatten_json(nested_json, ignore: list = None):
        flattened_json = {}
        if ignore is None:
            ignore = []

        def flatten(x, name=""):
            if (type(x) is dict) and (name[:-1] not in ignore):
                for a in x:
                    flatten(x[a], name + a + "_")
            else:
                flattened_json[name[:-1]] = x

        flatten(nested_json)
        return flattened_json
