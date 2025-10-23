# Visual Flow Comparison

## BEFORE: Single Outbid Notification

```
Existing Orders (Sell):
┌─────────────────────────────────────┐
│ Order 1: 100 coins (user1)          │
│ Order 2:  95 coins (user2)          │
│ Order 3:  90 coins (user3) ← TOP    │
└─────────────────────────────────────┘

New Order: 85 coins (user4) arrives
              ↓
         Check for outbids
              ↓
    Find FIRST outbid order
              ↓
        Notify user3 only ❌
              ↓
  user1 and user2 NOT notified ❌
```

**Problem**: Users 1 and 2 were also outbid but received no notification!

---

## AFTER: Multiple Outbid Notifications

```
Existing Orders (Sell):
┌─────────────────────────────────────┐
│ Order 1: 100 coins (user1) ✓        │
│ Order 2:  95 coins (user2) ✓        │
│ Order 3:  90 coins (user3) ✓ TOP    │
└─────────────────────────────────────┘

New Order: 85 coins (user4) arrives
              ↓
    GetAllOutbidOrders()
              ↓
   Returns: [Order3, Order2, Order1]
              ↓
      Notify ALL users ✅
      ┌──────┼──────┐
      ↓      ↓      ↓
   user3  user2  user1
      ↓      ↓      ↓
  Mark as notified: HasBeenNotified = true
```

**Result**: All affected users are notified!

---

## RESTART SCENARIO: Preventing Spam

### Without Protection ❌

```
Program Restarts → Load old orders from DB
┌─────────────────────────────────────┐
│ Order 1: 100 coins (user1)          │
│ Order 2:  95 coins (user2)          │
│ Order 3:  90 coins (user3)          │
│ Order 4:  85 coins (user4) ← NEWEST │
└─────────────────────────────────────┘

User adds new order: 80 coins
              ↓
  All orders outbid again!
              ↓
  Users 1,2,3 notified AGAIN ❌
  (Even though they were already notified before restart)
```

### With Protection ✅

```
Program Restarts → Load old orders from DB
              ↓
    MarkOldOrdersAsNotified()
              ↓
┌─────────────────────────────────────┐
│ Order 1: 100 coins (user1) 🔕       │
│ Order 2:  95 coins (user2) 🔕       │
│ Order 3:  90 coins (user3) 🔕       │
│ Order 4:  85 coins (user4) 🔔 TOP   │ ← Only this one active
└─────────────────────────────────────┘
  HasBeenNotified: true    false

User adds new order: 80 coins
              ↓
  GetAllOutbidOrders() filters by HasBeenNotified
              ↓
  Only user4 notified ✅
  (Others already marked as notified)
```

---

## Order Book State Transitions

### Normal Operation Flow

```
1. New Order Arrives
   ├→ Add to order book
   ├→ GetAllOutbidOrders()
   ├→ For each outbid order:
   │   ├→ SendOutbidNotification()
   │   ├→ Set HasBeenNotified = true
   │   └→ UpdateInDb()
   └→ InsertToDb(new order)
```

### Restart Flow

```
1. Program Starts
   ├→ Load() method called
   ├→ Load all orders from DB
   ├→ Add orders to in-memory cache
   ├→ MarkOldOrdersAsNotified()
   │   ├→ For each item's order book:
   │   │   ├→ Find top sell order (lowest price)
   │   │   ├→ Mark all others: HasBeenNotified = true
   │   │   ├→ Find top buy order (highest price)
   │   │   └→ Mark all others: HasBeenNotified = true
   └→ Ready for new orders
```

---

## Notification State Machine

```
Order Created
     ↓
[NOT_NOTIFIED] ← HasBeenNotified = false
     ↓
     │ (Outbid by new order)
     ↓
SendOutbidNotification()
     ↓
[NOTIFIED] ← HasBeenNotified = true
     ↓
     │ (Further outbids ignored)
     ↓
No more notifications for this order ✅
```

---

## Database Schema

### Old Schema
```
order_book
├── item_id (partition key)
├── timestamp (clustering key)
├── user_id (clustering key)
├── amount
├── is_sell
├── player_name
└── price_per_unit
```

### New Schema
```
order_book
├── item_id (partition key)
├── timestamp (clustering key)
├── user_id (clustering key)
├── amount
├── is_sell
├── player_name
├── price_per_unit
└── has_been_notified ← NEW FIELD
```

---

## Key Algorithm: GetAllOutbidOrders()

### For SELL Orders (Lower is Better)
```
Current Orders: [100, 95, 90, 85]
New Order: 80
         ↓
Filter: price > 80 AND HasBeenNotified = false
         ↓
Result: [100, 95, 90, 85] ← All are worse than 80
         ↓
Sort: Ascending (closest first)
         ↓
Final: [85, 90, 95, 100]
```

### For BUY Orders (Higher is Better)
```
Current Orders: [50, 55, 60, 65]
New Order: 70
         ↓
Filter: price < 70 AND HasBeenNotified = false
         ↓
Result: [50, 55, 60, 65] ← All are worse than 70
         ↓
Sort: Descending (closest first)
         ↓
Final: [65, 60, 55, 50]
```

---

## Example Scenarios

### Scenario 1: Gradual Undercutting
```
Time | Order        | Action                    | Notifications
-----|--------------|---------------------------|---------------
T1   | User1: $100  | Add order                 | None
T2   | User2: $95   | Add order                 | User1 notified
T3   | User3: $90   | Add order                 | User2 notified (User1 already notified)
T4   | User4: $85   | Add order                 | User3 notified (User1,2 already notified)
```

### Scenario 2: Big Undercut
```
Time | Order        | Action                    | Notifications
-----|--------------|---------------------------|---------------
T1   | User1: $100  | Add order                 | None
T2   | User2: $95   | Add order                 | User1 notified
T3   | User3: $90   | Add order                 | User2 notified
T4   | User4: $70   | Add order (BIG undercut!) | User3 notified (only)
                                                  (User1,2 already notified)
```

### Scenario 3: Restart
```
Time | Order        | Action                    | Notifications
-----|--------------|---------------------------|---------------
T1   | User1: $100  | Loaded from DB            | None
T2   | User2: $95   | Loaded from DB            | None
T3   | User3: $90   | Loaded from DB (TOP)      | None
     |              | MarkOldOrdersAsNotified() | User1,2 marked
T4   | User4: $85   | Add order                 | User3 notified (only)
                                                  (User1,2 marked as notified)
```

This ensures users aren't spammed with notifications for orders they were already notified about before the restart! ✅
