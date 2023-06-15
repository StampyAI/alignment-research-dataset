from .greaterwrong import GreaterWrong

GREATERWRONG_REGISTRY = [
    GreaterWrong(
        name="lesswrong",
        base_url='https://www.greaterwrong.com',
        start_year=2005,
        min_karma=1,
    ),
    GreaterWrong(
        name="eaforum",
        base_url='https://ea.greaterwrong.com',
        start_year=2011,
        min_karma=1,
    )
]
