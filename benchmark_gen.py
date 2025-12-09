import csv
import random
import sys


def main():
    if len(sys.argv) > 1:
        out_path = sys.argv[1]
    else:
        out_path = "benchmark-10000-hun.csv"

    clients = [f"C{i}" for i in range(1, 101)]
    nodes_str = "[n1, n2, n3, n4, n5]"

    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Set Number", "Transactions", "Live Nodes"])
        set_num = 1
        for i in range(10000):
            if i == 0:
                set_col = set_num
                live_nodes_col = nodes_str
            else:
                set_col = ""
                live_nodes_col = ""
            sender = random.choice(clients)
            receiver = random.choice(clients)
            while receiver == sender:
                receiver = random.choice(clients)
            amount = random.randint(1, 5)
            txn = f"({sender}, {receiver}, {amount})"
            w.writerow([set_col, txn, live_nodes_col])


if __name__ == "__main__":
    main()
