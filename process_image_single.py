import os
import time
import sys
from PIL import Image, ImageFilter, ImageOps, ImageEnhance
import numpy as np
import random


def color_jitter(image):
    enhancer = ImageEnhance.Color(image)
    factor = np.random.uniform(0.5, 1.5)  # Adjust the range based on your preference
    return enhancer.enhance(factor)


def augment_image(image):
    # Random horizontal or vertical flip
    if random.choice([True, False]):
        flipped_image = image.transpose(Image.FLIP_LEFT_RIGHT)
    else:
        flipped_image = image.transpose(Image.FLIP_TOP_BOTTOM)

    # Adjust brightness with a random factor between 0.8 and 1.2
    enhancer = ImageEnhance.Brightness(flipped_image)
    brightness_factor = np.random.uniform(0.8, 1.2)
    augmented_image = enhancer.enhance(brightness_factor)

    return augmented_image


def preprocess_image(input_path, output_directory, augmentation_probability=0.3):
    try:
        image = Image.open(input_path)

        # Apply color jittering
        jittered_image = color_jitter(image)

        # Apply image augmentation with probability
        if random.random() < augmentation_probability:
            augmented_image = augment_image(jittered_image)
        else:
            augmented_image = jittered_image

        # Convert to grayscale
        grayscale_image = augmented_image.convert('L')

        # Apply Gaussian blurring
        blurred_image = grayscale_image.filter(ImageFilter.GaussianBlur(radius=2))

        # Apply histogram equalization
        equalized_image = ImageOps.equalize(blurred_image)

        # Calculate the scaling factor for upscaling
        base_size = 512
        max_dimension = max(equalized_image.size)
        scaling_factor = base_size / max_dimension

        # Upscale the equalized image
        new_width = int(equalized_image.size[0] * scaling_factor)
        new_height = int(equalized_image.size[1] * scaling_factor)
        final_image = equalized_image.resize((new_width, new_height), Image.LANCZOS)

        # Create an output filename based on the input image name
        filename = os.path.basename(input_path)
        output_path = os.path.join(output_directory, f"{os.path.splitext(filename)[0]}_processed.jpg")

        final_image.save(output_path)
        print(f"Processed: {output_path}")
    except Exception as e:
        print(f"Error processing {input_path}: {e}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py input_image output_directory")
        sys.exit(1)

    input_image = sys.argv[1]
    output_directory = sys.argv[2]

    os.makedirs(output_directory, exist_ok=True)

    preprocess_image(input_image, output_directory)
