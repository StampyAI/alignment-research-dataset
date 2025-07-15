from .greaterwrong import GreaterWrong

GREATERWRONG_REGISTRY = [
    GreaterWrong(
        name="lesswrong",
        base_url="https://www.lesswrong.com",
        start_year=2005,
        min_karma=1,
        af=False,
    ),
    GreaterWrong(
        name="alignmentforum",
        base_url="https://www.alignmentforum.org",
        start_year=2009,
        min_karma=1,
        af=True,
    ),
    GreaterWrong(
        name="eaforum",
        base_url="https://forum.effectivealtruism.org",
        start_year=2011,
        min_karma=1,
        af=False,
    ),
]
