from db_dict import DBDict

# path of db and name of the table in DB
example_dict = DBDict("db_example.db", "example")

example_dict["key1"] = "content here 1"
example_dict["key2"] = "content here 2"

for i in example_dict.items():
    print(f"Key: {i[0]}\tValue: {i[1]}")

example_dict.pop("key1")

if "key1" in example_dict:
    print("It exists!")
else:
    print("It not exists!")

print(example_dict["key2"])