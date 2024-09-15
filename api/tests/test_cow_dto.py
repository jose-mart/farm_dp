import pytest
from datetime import date
from models.cow.CowDTO import CowDTO

def test_valid_cow_dto():
    cow = CowDTO(name="Bessie", birthdate=date(2020, 1, 1))
    assert cow.name == "Bessie"
    assert cow.birthdate == date(2020, 1, 1)

def test_valid_cow_dto_from_string():
    cow = CowDTO(name="Bessie", birthdate="2020-01-01")
    assert cow.name == "Bessie"
    assert cow.birthdate == date(2020, 1, 1)

def test_invalid_birthdate_format():
    with pytest.raises(ValueError) as excinfo:
        CowDTO(name="Bessie", birthdate="01-01-2020")
    assert "birthdate string is not in the correct format 'YYYY-MM-DD'" in str(excinfo.value)

def test_birthdate_not_a_date():
    with pytest.raises(TypeError) as excinfo:
        CowDTO(name="Bessie", birthdate=123456)
    assert "birthdate must be of type 'date'" in str(excinfo.value)

def test_birthdate_in_the_future():
    future_date = date.today().replace(year=date.today().year + 1)
    with pytest.raises(ValueError) as excinfo:
        CowDTO(name="Bessie", birthdate=future_date)
    assert "birthdate cannot be in the future" in str(excinfo.value)

if __name__ == "__main__":
    pytest.main()