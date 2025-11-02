"""
This is a boilerplate pipeline 'data_transformation'
generated using Kedro 1.0.0
"""
import pandas as pd


def normalise_recipe_df(raw_recipes_df:pd.DataFrame):
    """Normalise the recipe dataframe into three datasets.

    Args:
        raw_recipes_df (pd.DataFrame):
            Raw recipe data taken from Kaggle dataset.

    Returns:
        recipe (pd.DataFrame):
            Dataset containing the following columns:
                - ``index``
                - ``Title``
                - ``Instructions``
        recipe_ingredients (pd.DataFrame):
            Dataset containing the following columns:
                - ``index``
                - ``recipe_index``
                - ``ingredients``
        recipe_image (pd.DataFrame):
            Dataset containing the following columns:
                - ``index``
                - ``recipe_index``
                - ``Image_Name``
    """
    recipe = raw_recipes_df[['index', 'Title', 'Instructions']]
    recipe_ingredients = (
        raw_recipes_df[['index', 'Cleaned_Ingredients']]
        .rename(
            columns={
                'index' : 'recipes_index',
                'Cleaned_Ingredients' : 'ingredients'
            }
        )
        # make sure index is not dropped
        .reset_index(drop=False)
    )
    recipe_image = (
        raw_recipes_df[['index', 'Image_Name']]
        .rename(columns={'index' : 'recipes_index'})
        # make sure index is not dropped
        .reset_index(drop=False)
    )
    return recipe, recipe_ingredients, recipe_image


def normalise_recipe_image_name(recipe_image:pd.DataFrame):
    """Add file extension to recipe image name.

    Args:
        recipe_image (pd.DataFrame):
            Dataset containing the following columns:
                - ``index``
                - ``recipe_index``
                - ``Image_Name``

    Returns:
        recipe_image (pd.DataFrame):
            dataframe with the ``Image_Name`` column
            now with ``.jpg`` file extension appended.
    """
    recipe_image.loc[:, 'Image_Name'] = (
        recipe_image['Image_Name'].str.cat(
            ['jpg'] * len(recipe_image),
            sep='.'
        )
    )
    return recipe_image



def ingredients_str_to_list(ingredients_series:pd.Series):
    return (
        ingredients_series.str.replace('[', '')
        .str.replace(']', '')
        .str.split("\\'")
    )

def get_exploded_ingredients_per_recipe_index(recipe_ingredients:pd.DataFrame):
    """Explode the list of ingredients into separate rows.

    ``ingredients`` column is expected to contain strings of the form:

    ..code-block:: python
        '[\'ingredient1\', \'ingredient2\', \'ingredient3\']'

    Args:
        recipe_ingredients (pd.DataFrame):
            Dataset containing the following columns:
                - ``index``
                - ``recipe_index``
                - ``ingredients``

    Returns:
        exploded_ingredients_per_recipe (pd.DataFrame):
            Dataset containing the following columns:
                - ``index``
                - ``recipe_index``
                - ``ingredients``
            where each ingredient per recipe is in a separate row.
    """
    # explode the list of ingredients into separate rows
    exploded_ingredients_per_recipe = recipe_ingredients.explode('ingredients')

    # remove leading and trailing whitespace from all entries
    exploded_ingredients_per_recipe.loc[:, 'ingredients'] = exploded_ingredients_per_recipe['ingredients'].str.strip()

    # remove rows containing str artefacts from the string split operation upstream
    empty_str_mask = exploded_ingredients_per_recipe['ingredients'] == ""
    comma_only_mask = exploded_ingredients_per_recipe['ingredients'] == ","
    exploded_ingredients_per_recipe = exploded_ingredients_per_recipe[~empty_str_mask & ~comma_only_mask]

    return (
        exploded_ingredients_per_recipe
        # replace the inherited recipes index with new row index
        .reset_index(drop=True)
        # make the newly created index a column
        .reset_index(drop=False)
    )

def normalise_recipe_ingredients(recipe_ingredients:pd.DataFrame):
    recipe_ingredients.loc[:, 'ingredients'] = ingredients_str_to_list(recipe_ingredients['ingredients'])
    ingredients_per_recipe = get_exploded_ingredients_per_recipe_index(recipe_ingredients)
    return ingredients_per_recipe

