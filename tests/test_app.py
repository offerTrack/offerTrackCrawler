import app


def test_hello_world():
    assert app.hello_world() == "Hello, World!"


def test_main_exists():
    assert callable(app.main)
