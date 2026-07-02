# 📒 Python dict as a database

## 📦 Installation
You can install db_dict via pip:
```
pip install git+https://github.com/PlushkaNet/db_dict.git
```

**Sync usage example:**
```
from db_dict.sync import DBDict
from dataclasses import dataclass

@dataclass
class Data:
    name: str
    data: int

d = DBDict("test.db", "example")

d["data"] = Data("data", 1)
d[1] = 1

print(d["data"])
```

### ❗ Asyncio module currently not fully implemented

**To get complete examples, check out <a href="example.py">sync</a> and <a href="async_example.py">async</a> example files**
