import logging
import sys
import tkinter
import tkinter.simpledialog
from threading import Thread
from tkinter import messagebox

import discovery
from client import Client

logging.basicConfig(format='[%(asctime)s] %(levelname)s (%(name)s) %(message)s', level=logging.DEBUG)

client = None


def user_interface():
    # ############################################################################################
    def receive(msg):
        if isinstance(msg, str):
            msg_list.insert(tkinter.END, msg)
        elif isinstance(msg, list):
            for mg in msg:
                msg_list.insert(tkinter.END, mg)
        else:
            raise NotImplementedError('received messages by send_test | history can be only string | list!')

    def init_client(nickname):
        global client
        # networking init
        primary = discovery.find_primary()
        if primary is not None:
            client = Client(primary.serverAddress(), nickname)
            client.onreceive = receive
            client.start()
        else:
            messagebox.showerror('Error!', 'Server not available!')
            sys.exit(1)

    def send(_=None):
        assert type(my_msg) == tkinter.StringVar, 'my_msg corrupted!'
        client.sendMessage("<{}> {}".format(client._nickname, my_msg.get()))
        my_msg.set("")

    def on_closing():
        """This function is to be called when the window is closed."""
        assert type(my_msg) == tkinter.StringVar, 'my_msg corrupted!'
        if messagebox.askokcancel("Quit", "Do you want to quit?"):
            top.destroy()
            client.shutdown()

    # ############################################################################################
    top = tkinter.Tk()
    top.title("DS Chat")

    top.withdraw()
    nickname = tkinter.simpledialog.askstring("Nickname", "Enter nickname:", parent=top)
    if nickname:
        top.title("DS Chat - " + nickname)
        init_client(nickname)
        top.deiconify()
    else:
        return

    messages_frame = tkinter.Frame(top)
    my_msg = tkinter.StringVar()  # For the messages to be sent.

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
