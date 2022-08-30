# Transform and Clean Online Orders

In this activity, you'll transform and clean a dataset that consists of online orders.

## Instructions

1. Create six columns from the one column that’s named “orderId,productName,category,sellingPrice,shipping,totalPrice”.

2. Create a new DataFrame with the six columns, and then to populate those columns, split the row values on the comma (',') delimiter.

3. Convert the "sellingPrice" and "totalPrice" columns to the `float` data type.

4. Calculate the shipping price of each product, and then add it to a new column named "shippingPrice".

5. Reorder the columns to match the final DataFrame, as the following image shows:

    ![A screenshot depicts the final DataFrame.](Images/A1-online_orders_df.png)

    In the preceding image, notice that the columns appear in the following order: “orderId”, “productName”, “category”, “sellingPrice”, “shippingPrice”, “totalPrice”, “shipping”.

## References

Data for this dataset was created by 2U for the Netflix Pathways Bootcamp, and is intended for educational purposes only.

- - -

Copyright 2022 2U. All Rights Reserved.
