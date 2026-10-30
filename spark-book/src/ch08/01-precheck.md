# Pre-check: what do you think you know?

<span class="tag">5 min</span>

Same rules as Chapter 7: answer from what you know now. Your answers are saved in this browser.

```quiz
{
  "id": "ch08-precheck",
  "title": "Chapter 8 pre-check",
  "questions": [
    {
      "q": "An SCD Type 2 load must expire a customer's current row AND insert their new version. Why do Delta implementations feed every changed customer into the MERGE a second time, with a NULL merge key?",
      "options": [
        "Because `whenNotMatchedInsert` only fires for source rows whose key is NULL, so without the extra copy a changed customer could never be inserted again.",
        "Because Delta evaluates insert clauses before update clauses, and the NULL-keyed copy forces the expiry to happen first so the two versions never overlap.",
        "Because each source row fires at most one MERGE clause; the NULL-keyed copy never matches, so it is inserted while the normal copy expires the old row.",
        "Because without the extra copy the MERGE would update the current row in place, and Delta's transaction log would then lose the `valid_from` of the old version."
      ],
      "answer": 2,
      "explain": "One source row → at most one action. A changed customer needs two actions (UPDATE the old row, INSERT the new one), so you send two rows. The copy with `merge_key = NULL` never matches anything (NULL = x is never true), so it falls through to `whenNotMatchedInsert`. The 'insert only fires for NULL keys' and 'inserts run before updates' options describe rules MERGE doesn't have. The transaction-log option is half-true: an in-place update would indeed destroy history, but in the table, not in the log."
    },
    {
      "q": "A fact table of orders is joined to an SCD Type 2 customer dimension on `customer_id` only, with no condition on the validity dates. What happens to the result?",
      "options": [
        "Each order is matched to the customer's current version, because Delta resolves the join using the `is_current` flag of the dimension.",
        "The join fails with an ambiguity error, because `customer_id` is not unique in the dimension and Spark refuses many-to-one joins on it.",
        "Orders placed before the customer's first `valid_from` are dropped, so history is undercounted but nothing is duplicated.",
        "Each order is repeated once per version of its customer, so totals are inflated for every customer who ever changed."
      ],
      "answer": 3,
      "explain": "Spark joins on what you tell it. A customer with 3 versions turns each of their orders into 3 rows. This is the most common SCD2 bug in reporting, and a row-count check before and after the join catches it."
    },
    {
      "q": "In which case is overwriting the old value (SCD Type 1) clearly the right choice, rather than just the easy one?",
      "options": [
        "A customer's loyalty tier, because marketing dashboards only ever filter on the tier a customer has today.",
        "A misspelled customer name being corrected, because the misspelling was never a true state of the customer.",
        "A product's list price, because finance can always recompute historical prices from the order lines.",
        "An employee's department, because HR already keeps the history of transfers in its own source system."
      ],
      "answer": 1,
      "explain": "Type 1 is right when the old value was **never true**: corrections, typos, data-entry fixes. The other three are real changes that someone will eventually ask about historically. 'Nobody reports on it today' and 'it exists somewhere else' are the usual excuses for losing history."
    }
  ]
}
```
