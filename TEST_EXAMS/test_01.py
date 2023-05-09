import pytest

class C:
    def __init__(self, balance = 0) -> None:
        self.balance = balance

    def f(self):
        return 1
    
    def g(self):
        return 2
    
    def spend_money(self, money_spend):
        if self.balance < money_spend:
            raise ValueError('You don"t have enough money')

    
@pytest.fixture
def c_instance():
    return C(50)

def test_f(c_instance):
    assert c_instance.f() == 1


def test_g(c_instance):
    assert c_instance.g() == 2


def test_spend_money(c_instance):
    with pytest.raises (ValueError) :
        c_instance.spend_money(100)
