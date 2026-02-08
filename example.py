from db_dict.sync import DBDict
# it is example of using sync DBDict

# path of db and name of the table in DB
example_dict = DBDict("db_example.db", "example")

example_dict["key1"] = "content here 1"
example_dict["key2"] = "content here 2"
example_dict["key3"] = True
example_dict["key4"] = False
example_dict["key5"] = 15
example_dict["key6"] = 91.8
example_dict["key7"] = [8, "hello"]
example_dict["key8"] = {"something":"something"}

for i in example_dict.items():
    print(f"Key: {i[0]}\tValue: {i[1]}")

example_dict.pop("key1")

if "key1" in example_dict:
    print("It exists!")
else:
    print("It not exists!")

print(example_dict["key2"])
print(example_dict["key3"])
print(example_dict["key4"])
print(example_dict["key5"])
print(example_dict["key6"])
print(example_dict["key7"])
print(example_dict["key8"])