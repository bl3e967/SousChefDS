"""
This is a boilerplate pipeline 'data_transformation'
generated using Kedro 1.0.0
"""

from kedro.pipeline import Node, Pipeline  # noqa
from .nodes import (
    normalise_recipe_df,
    normalise_recipe_image_name,
    normalise_recipe_ingredients
)


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            Node(
                normalise_recipe_df,
                inputs="raw_recipes_source",
                outputs=["recipe", "raw_recipe_ingredients", "raw_recipe_image"],
                name="normalise_recipe_df"
            ),

            ## normalise recipe image
            Node(
                normalise_recipe_image_name,
                inputs="raw_recipe_image",
                outputs="recipe_image",
                name="normalise_recipe_image_name"
            ),

            ## normalise recipe
            Node(
                normalise_recipe_ingredients,
                inputs="raw_recipe_ingredients",
                outputs="recipe_ingredients",
                name="normalise_recipe_ingredients"
            ),
        ]
    )
