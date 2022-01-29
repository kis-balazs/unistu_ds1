import time
import tkinter
import random
from threading import Thread
from tkinter import messagebox
 

def user_interface():
    def send():
        assert type(my_msg) == tkinter.StringVar, 'my_msg corrupted!'
        print('sending: {}'.format(my_msg.get()))
    
    def on_closing():
        """This function is to be called when the window is closed."""
        assert type(my_msg) == tkinter.StringVar, 'my_msg corrupted!'
        if messagebox.askokcancel("Quit", "Do you want to quit?"):
            top.destroy()
        my_msg.set("{quit}")
        send()
    
    def receive():
        """Handles receiving of messages."""
        while True:
            try:
                sample_messages = ['Hello', 'Hi there!', 'Ceao bella!', 'Good day!']
                sample_users = ['Giovanni', 'Mark', 'Paul', 'Jake']
                msg = '{}: {}'.format(random.choice(sample_users), random.choice(sample_messages))
                msg_list.insert(tkinter.END, msg)
    
                time.sleep(random.randint(1, 5))
            except OSError:  # Possibly client has left the chat.
                break
    
    
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
    
    receive_thread = Thread(target=receive)
    receive_thread.start()
    tkinter.mainloop()  # Starts GUI execution.
    

if __name__ == '__main__':
    Thread(target=user_interface).start()
