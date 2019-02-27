--Took the composite key of id_ingredient and ingredient_name, as with id_ingredient("1") there are two ingredient_name present(Potato and Sweet Potato)
--Just shown the three columns, in case all the columns need to be shown then need to place another self join and all columns will be shown.
--Removed the id_ingredient and ingredient_name if the ingredient is deleted, like Tomatoes
select y.id_ingredient,y.ingredient_name,max(y.timestamp1) from ingredients y where not exists(select 1 from  ingredients x where x.id_ingredient = y.id_ingredient and x.ingredient_name = y.ingredient_name and x.deleted = 1)
group by y.id_ingredient,y.ingredient_name