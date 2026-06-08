# Python dict's like class for have a dict experience with data bases!

## Installation
You can install db_dict using pip:
```
pip install git+https://github.com/PlushkaNet/db_dict.git
```

<b>Sync usage example:</b>
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

<h3>! Asyncio module currently not full implemented</h3>

<b> To get complete examples, check out <a href="example.py">sync</a> and <a href="async_example.py">async</a> example files