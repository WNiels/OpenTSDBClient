class OpenTSDB():
    def __init__(self, url: str, port: int):
        """Initializes the OpenTSDB class.

        Args:
            url (str): OpenTSDB server url.
            port (int): OpenTSDB server port.
        """
        self.__url = url
        self.__port = port
        self.__complete_url = f'{self.__url}:{self.__port}'

    def aggregators(self) -> list:
        """Returns a list of all available aggregators.
        The list is requested from the set OpenTSDB server.

        Returns:
            list: List of all available aggregators.
        """
        url = f'{self.__complete_url}/api/aggregators'
        return requests.get(url=url).json()
    pass

if __name__ == '__main__':
    pass