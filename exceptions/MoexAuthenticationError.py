class MoexAuthenticationError(Exception):

    def __init__(self, message="Moex authentication failed."):
        super().__init__(message)
