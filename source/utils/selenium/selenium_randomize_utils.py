import random
import string
from time import sleep
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys

class SeleniumRandomUtils:

    def __init__(self, driver):
        self.driver = driver

    def element_pointer_movement(self, element):
        """
        Move cursor to a an element position.
        The coordinates to move to are decided based on the element's width and height.
        INPUT: element - the element on which pointer movement should happen
        """
        action = ActionChains(self.driver)
        x = element.size['width']
        y = element.size['height']
        hover_x_axis = random.randrange(x)
        hover_y_axis = random.randrange(y)
        action.move_to_element_with_offset(element, hover_x_axis, hover_y_axis).perform()
        sleep(random.random())

    def generate_random_curve(self):
        """Generate a random mouse pointer curve on the page."""
        generate_curve = random.choice([True, False])

        if generate_curve:

            try:
                coordinates_list = [
                    [8, 1],
                    [6, 1],
                    [4, 1],
                    [2, 1],
                    [1, 1],
                    [1, 2],
                    [1, 4],
                    [1, 6],
                    [1, 8],
                    [-1, 0],
                    [-1, 0],
                    [-1, 0],
                    [-1, 0],
                    [-2, -1],
                    [-3, -2],
                    [-4, -3],
                    [-5, -4]
                ]
                # shuffle the list elements order
                random.shuffle(coordinates_list)

                for each in coordinates_list:

                    action = ActionChains(self.driver)
                    action.move_by_offset(each[0], each[1])
                    action.perform()
                    sleep(random.random())
            except Exception as e:
                print('error while generating a curve', str(e))

    def randomize_input_values(self, element, input_string, key_to_press):
        """
        This will insert and clear additional characters
        randomly in between the values that need to be filled.
        INPUT:  element- input element
                input_string- string value to enter in the input element
                key_to_press- Keyboard key to be pressed after input is done
        OUTPUT:
                Enter random values into input element and removes those randomly
                 entered values and enters the actual values into input element
        """
        all_characters = string.ascii_letters
        for each_character in input_string:
            action = ActionChains(self.driver)
            # randomly choose one value
            generate_wrong_character = random.choice([True, False])
            # if true, then generate wrong character and delete it
            if generate_wrong_character:
                element.send_keys(random.choice(all_characters))
                # generate random float number between the given range
                sleep(random.uniform(2, 3))
                element.send_keys(Keys.BACKSPACE)
                # generate a random float number 0 and 1
                sleep(random.random())
            # hold shift key while typing a character which is of uppercase
            if each_character.isupper():
                action.key_down(Keys.SHIFT).perform()
            element.send_keys(each_character)
            sleep(random.random())
        element.send_keys(key_to_press)
