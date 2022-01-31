from pydoc import cli
import time
import tkinter
import random
import logging
from threading import Thread
from tkinter import messagebox

import discovery
from client import Client

logging.basicConfig(format='[%(asctime)s] %(levelname)s (%(name)s) %(message)s', level=logging.DEBUG)

def user_interface():
    # ############################################################################################
    client = None

    def receive(msg):
        msg_list.insert(tkinter.END, msg)

    def init_client():
        global client
        # networking init
        primary = discovery.find_primary()
        if primary is not None:
            client = Client(primary, "nickname")
            client.onreceive = receive
            client.start()
        else:
            # alert
            pass

    def send(a=None):
        global client
        assert type(my_msg) == tkinter.StringVar, 'my_msg corrupted!'
        client.sendMessage(my_msg.get())

    def on_closing():
        """This function is to be called when the window is closed."""
        assert type(my_msg) == tkinter.StringVar, 'my_msg corrupted!'
        if messagebox.askokcancel("Quit", "Do you want to quit?"):
            top.destroy()

    # ############################################################################################
    init_client()

    top = tkinter.Tk()
    top.title("Chatter")
    
    messages_frame = tkinter.Frame(top)
    my_msg = tkinter.StringVar()  # For the messages to be sent.
    my_msg.set("Type your messages here.")
    scrollbar = tkinter.Scrollbar(messages_frame)  # To navigate through past messages.
    # Following will contain the messages.
    msg_list = tkinter.Listbox(messages_frame, height=15, width=50, yscrollcommand=scrollbar.set)
    scrollbar.pack(side=tkinter.RIGHT, fill=tkinter.Y)
    msg_list.pack(side=tkinter.LEFT, fill=tkinter.BOTH)
    msg_list.pack()
    messages_frame.pack()
    
    entry_field = tkinter.Entry(top, textvariable=my_msg)
    entry_field.bind("<Return>", send)
    entry_field.pack()
    send_button = tkinter.Button(top, text="Send", command=send)
    send_button.pack()

    top.protocol("WM_DELETE_WINDOW", on_closing)

    tkinter.mainloop()  # Starts GUI execution.


if __name__ == '__main__':
    Thread(target=user_interface).start()
