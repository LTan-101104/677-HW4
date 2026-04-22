from config.enums import Item


DEFAULT_STOCK = 3

# Fixed price table — "purchase prices are determined a priori and are not negotiable" (spec).
PRICES: dict[Item, float] = {
    Item.SALT: 5.0,
    Item.FISH: 8.0,
    Item.BOAR: 20.0,
}

# Trader keeps this fraction of each sale as commission; seller gets the rest.
TRADER_COMMISSION = 0.10

#! config for main cycle of running in main.py

# Only the first elected trader resigns; subsequent traders run to completion.
# Bully always re-elects the highest-PID peer alive, so chained resignations
# would just bounce between the same two candidates and add nothing.
FIRST_TRADER_RESIGNS = True
RESIGN_AFTER = (2.0, 4.0)
# Cooldown a resigning trader sits out of elections.
RESIGN_YIELD = 3.0
