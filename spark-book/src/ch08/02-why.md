# What problem SCD solves

<span class="tag">10 min</span>

## Facts and dimensions

Analytical models split data into two kinds of tables:

- **Facts** record *events*: an order, a payment, an inspection. They're append-only, there are many
  of them, and each one has a date.
- **Dimensions** describe the *entities* those events are about: customers, products, stores,
  employees. There are few of them, they're wide, and they're **mutable**. A customer moves city, a
  product changes category, a store changes manager.

The attributes of a dimension change *slowly*, which gives the problem its name. The question SCD
answers is: **when an attribute changes, what do we do with the old value?**

It matters because facts get reported *through* dimensions. "Revenue by city" means "join orders to
customers and group by city". But which city: where the customer lived *when they ordered*, or where
they live *now*? Both are legitimate questions, and you can only answer the first if you kept history.

## The SCD types

| Type | Strategy | History? | Typical use |
|---|---|---|---|
| **0** | never change after insert | none needed | birth date, original signup channel |
| **1** | overwrite | ❌ none | corrections and typos; attributes nobody will ever analyse historically |
| **2** | close the old row, insert a new one (`valid_from` / `valid_to` / `is_current`) | ✅ full | "as it was at the time" reporting: **the important one** |
| **3** | an extra column with the previous value | ⚠️ one step | "current vs previous region" comparisons, a re-org transition period |
| **4** | current table (Type 1) plus a separate history table | ✅ full, separate | huge dimensions where 99% of queries need only the current state |
| **6** | 1 + 2 + 3: SCD2 rows that also carry the *current* value | ✅ | report by historical **and** current value in one join |

What Type 2 looks like for one customer who moved twice:

```text
customer_id  name   city    valid_from  valid_to    is_current
1            Alice  Kyiv    2026-10-01  2026-10-05  false
1            Alice  Dnipro  2026-10-05  2026-10-10  false
1            Alice  Kyiv    2026-10-10  9999-12-31  true
```

<div class="realworld">

### Where you'll meet this at work

- **Sales territories and commissions.** A rep's region changed in March. Q1 commissions must be
  computed with the *old* region. Type 2 on the rep dimension.
- **Customer segmentation.** "Did customers upgraded to *gold* actually spend more after the
  upgrade?" You need the tier as it was before and after: Type 2.
- **Product catalog.** A product moved from *Accessories* to *Electronics*. Last year's category
  report must not change retroactively: Type 2 on category. A typo fix in the product name: Type 1.
- **Regulated industries.** Insurance (policy terms at the time of a claim), banking (a customer's
  risk rating when a loan was approved), healthcare (a provider's affiliation on the date of service).
  Auditors ask "what did you know, and when?", and the SCD2 dimension is the answer.
- **HR.** Headcount by department over time, with every transfer: Type 2. A department's *current*
  name on old reports after a rename: Type 6.
- **Public data.** NYC restaurant grades change after every inspection. You'll build exactly this
  dimension in the [capstone](./08-capstone.md).

</div>

<div class="pause">

**Pause and predict** <span class="tag">2 min</span>

Day 2's load sends Carol again, **unchanged**. In a naïve Type 1 MERGE (`whenMatchedUpdateAll()`
with no condition), what happens to her row? What would that do to Chapter 7's Change Data Feed
consumers? And in Type 2, how many rows would Carol end up with after 30 daily loads if you didn't
detect "unchanged"?

</div>
