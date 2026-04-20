import random
# from config.constant import DEFAULT_STOCK

ITEMS = ["fish", "salt", "boar"]


def assign_roles(n):
    """
    Assign random roles to n peers, guaranteeing at least 1 buyer
    and at least 1 seller.

    Returns:
        list of (role, item) tuples — item is None for buyers.
    """
    roles = []
    for _ in range(n):
        if random.random() < 0.5:
            roles.append(("buyer", None))
        else:
            roles.append(("seller", random.choice(ITEMS)))

    # Guarantee at least one buyer
    if not any(r == "buyer" for r, _ in roles):
        idx = random.randrange(n)
        roles[idx] = ("buyer", None)

    # Guarantee at least one seller
    if not any(r == "seller" for r, _ in roles):
        # Pick a peer that isn't the only buyer
        buyer_indices = [i for i, (r, _) in enumerate(roles) if r == "buyer"]
        if len(buyer_indices) > 1:
            idx = random.choice(buyer_indices)
        else:
            idx = random.randrange(n)
        roles[idx] = ("seller", random.choice(ITEMS))

    return roles
