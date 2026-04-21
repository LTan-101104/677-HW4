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
