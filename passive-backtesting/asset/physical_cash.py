from asset import Asset


class PhysicalCash(Asset):
    pass


class PhysicalEuro(PhysicalCash):
    
    def __init__(self, amount: float) -> None:
        self.amount = amount
    

    def __repr__(self):
        return f"{self.__class__.__name__}(amount={self.amount})"
