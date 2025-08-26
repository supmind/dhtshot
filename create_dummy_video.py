from PIL import Image

# Create a 2x2 pixel black image
img = Image.new('RGB', (2, 2), 'black')
img.save('black.png')
