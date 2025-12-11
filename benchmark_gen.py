import argparse
import csv
import random


def account_range(cluster):
    start = cluster * 3000 + 1
    end = (cluster + 1) * 3000
    return start, end


def pick_account(rng, cluster, distribution, skew):
    start, end = account_range(cluster)
    if distribution == "skewed" and skew > 0 and rng.random() < skew:
        hot_size = max(1, (end - start + 1) // 100)
        return rng.randint(start, start + hot_size - 1)
    return rng.randint(start, end)


def gen_transaction(rng, read_pct, cross_pct, distribution, skew):
    if rng.random() < read_pct:
        cluster = rng.randrange(3)
        account = pick_account(rng, cluster, distribution, skew)
        return ("read", account, None, None)
    cross = rng.random() < cross_pct
    if cross:
        src_cluster = rng.randrange(3)
        dst_cluster = (src_cluster + 1 + rng.randrange(2)) % 3
    else:
        src_cluster = rng.randrange(3)
        dst_cluster = src_cluster
    sender = pick_account(rng, src_cluster, distribution, skew)
    receiver = pick_account(rng, dst_cluster, distribution, skew)
    while receiver == sender:
        receiver = pick_account(rng, dst_cluster, distribution, skew)
    amount = rng.randint(1, 5)
    return ("write", sender, receiver, amount)


def build_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="benchmark.csv")
    parser.add_argument("--txns", type=int, default=1000)
    parser.add_argument("--read-pct", type=int, default=50)
    parser.add_argument("--cross-pct", type=int, default=50)
    parser.add_argument("--skew", type=float, default=0.0)
    parser.add_argument("--distribution", choices=["uniform", "skewed"], default="uniform")
    parser.add_argument("--seed", type=int)
    return parser.parse_args()


def main():
    args = build_args()
    rng = random.Random(args.seed)
    total = max(1, args.txns)
    read_pct = min(max(args.read_pct, 0), 100) / 100
    cross_pct = min(max(args.cross_pct, 0), 100) / 100
    skew = min(max(args.skew, 0.0), 1.0)
    distribution = args.distribution

    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Set Number", "Transactions", "Live Nodes"])
        live_nodes = "[n1, n2, n3, n4, n5, n6, n7, n8, n9]"
        for i in range(total):
            if i == 0:
                set_col = 1
                live_nodes_col = live_nodes
            else:
                set_col = ""
                live_nodes_col = ""
            kind, a, b, amt = gen_transaction(rng, read_pct, cross_pct, distribution, skew)
            if kind == "read":
                txn = f"({a})"
            else:
                txn = f"({a}, {b}, {amt})"
            w.writerow([set_col, txn, live_nodes_col])


if __name__ == "__main__":
    main()
