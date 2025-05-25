import re


class StringUtils(object):
    @staticmethod
    def upper(s: str) -> str:
        return s.upper()

    @classmethod
    def isblank(cls, string):
        return string is None or str(string).strip().lower() in {"none", "null", ""}

    @staticmethod
    def build_string(
        *items,
        separator: str = "",
    ):
        return separator.join(filter(lambda item: not StringUtils.isblank(item), items))

    @staticmethod
    def split_file_name(file_name: str):
        pattern = r"^(.*?)([_-])([A-Za-z_-]+)(\..+)$"
        match = re.match(pattern, file_name)
        if match:
            return match.group(1) + match.group(2), match.group(3), match.group(4)
        return None, None, None

    @staticmethod
    def reformated_date_pattern(date_pattern: str):
        count = {}
        separators = {}
        for i, char in enumerate(date_pattern):
            if char in "-_":
                separators[i] = char
                continue
            count[char] = count.get(char, 0) + 1

        pattern = ""
        for i, key in enumerate(count.keys()):
            pattern += f"\\d{{{count[key]}}}" + (
                separators.get(i, "") if i in separators else ""
            )

        return pattern
