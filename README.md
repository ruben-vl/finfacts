# Passive Investing Backtests

Development repo for finance fact checking (FinFacts?)

The goal is to create a parameterized model that can simulate certain (passive) investment strategies on historical data.

## Virtual Env Setup

> [!NOTE]
> "Python Virtual Environments: A Primer" by [RealPython](https://realpython.com/python-virtual-environments-a-primer/)

Create venv with `python3 -m venv venv`, activate with `source venv/bin/activate`, deactivate with `deactivate`.

Install package in the venv with
```bash
python -m pip install <package-name>
```

Pin dependencies
```bash
python -m pip freeze > requirements.txt
```

Install dependencies in virtual environment
```bash
python -m pip install -r requirements.txt
```
