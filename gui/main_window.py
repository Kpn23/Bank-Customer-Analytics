import customtkinter as ctk
import requests
from tkinter import Text, Scrollbar, messagebox


class MainSystem(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Customer Analytics Platform")
        self.geometry("800x580")
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("dark-blue")

        # Create a tab view
        self.tabview = ctk.CTkTabview(self)
        self.tabview.pack(expand=True, fill="both")

        # Add tabs
        self.profile_tab = self.tabview.add("Customer Profile")
        self.account_tab = self.tabview.add("Account & Transaction Summary")

        # Create elements in the profile tab
        self.create_element_profile(self.profile_tab)

    def create_element_profile(self, parent=None):
        if parent is None:
            parent = self.profile_tab  # Default to profile_tab if no parent is provided
        
        # Clear existing widgets
        for widget in parent.winfo_children():
            widget.destroy()

        input_frame = ctk.CTkFrame(parent)
        input_frame.pack(fill="x", padx=10, pady=10)

        # Customer Name Entry
        self.customer_name_label = ctk.CTkLabel(input_frame, text="Customer Name:")
        self.customer_name_label.pack(anchor='w', padx=5, pady=(5, 0))
        
        self.customer_name_entry = ctk.CTkEntry(input_frame)
        self.customer_name_entry.pack(fill='x', padx=5, pady=(0, 5))

        # ID Number Entry
        self.id_number_label = ctk.CTkLabel(input_frame, text="ID Number:")
        self.id_number_label.pack(anchor='w', padx=5, pady=(5, 0))

        self.id_number_entry = ctk.CTkEntry(input_frame)
        self.id_number_entry.pack(fill='x', padx=5, pady=(0, 5))

        # Search Button
        self.search_button = ctk.CTkButton(input_frame, text="Search", command=self.search_customer)
        self.search_button.pack(pady=20)

    def search_customer(self):
        customer_name = self.customer_name_entry.get()
        id_number = self.id_number_entry.get()

        if not customer_name or not id_number:
            messagebox.showerror("Error", "Please enter both Customer Name and ID Number.")
            return

        try:
            api_url = f"http://127.0.0.1:8000/customers/?customer_name={customer_name}&id_number={id_number}"
            response = requests.get(api_url)
            response.raise_for_status()
            customer_data = response.json()

            if not customer_data:
                messagebox.showinfo("Info", "No customer found with the provided details.")
                return

            # Populate profile and account summary
            self.populate_customer_profile(customer_data)
            account_data = customer_data.get("Accounts", [])
            transaction_data = customer_data.get("Transactions", [])
            self.populate_account_summary(account_data, transaction_data)

        except requests.exceptions.RequestException as e:
            messagebox.showerror("Error", f"Error fetching data: {e}")

    def populate_customer_profile(self, data):
        for widget in self.profile_tab.winfo_children():
            widget.destroy()

        profile_container = ctk.CTkFrame(self.profile_tab)
        profile_container.pack(expand=True, fill="both", padx=10, pady=10)
        
        profile_container.grid_columnconfigure((0, 1, 2), weight=1)

        # Basic Information Frame
        basic_info_frame = ctk.CTkFrame(profile_container)
        basic_info_frame.grid(row=0, column=0, padx=10, pady=10, sticky='nsew')
        
        ctk.CTkLabel(basic_info_frame, text="Basic Information", font=("Arial", 14)).pack(anchor='w', padx=5)

        basic_info = data.get("Basic_Information", {})
        
        labels_and_keys = [
            ("Customer ID:", "Customer_ID"),
            ("Name:", "Name"),
            ("ID Number:", "ID_Number"),
            ("Age:", "Age")
        ]

        for label_text, key in labels_and_keys:
            label = ctk.CTkLabel(basic_info_frame, text=label_text)
            label.pack(anchor='w', padx=(5, 2), pady=(2))

            value_textbox = ctk.CTkTextbox(basic_info_frame, height=1)
            value_textbox.insert("end", str(basic_info.get(key, "N/A")))
            value_textbox.configure(state='disabled')
            value_textbox.pack(fill='x', padx=(2, 5), pady=(2))

        # Address Frame
        address_frame = ctk.CTkFrame(profile_container)
        address_frame.grid(row=0, column=1, padx=10, pady=10, sticky='nsew')

        ctk.CTkLabel(address_frame, text="Address Information", font=("Arial", 14)).pack(anchor='w', padx=5)

        address_info = data.get("Address_Information", {})
        
        labels_and_keys_address = [
            ("State:", "State"),
            ("City:", "City"),
            ("Street:", "Street")
        ]

        for label_text, key in labels_and_keys_address:
            label = ctk.CTkLabel(address_frame, text=label_text)
            label.pack(anchor='w', padx=(5, 2), pady=(2))

            value_textbox = ctk.CTkTextbox(address_frame, height=1)
            value_textbox.insert("end", str(address_info.get(key, "N/A")))
            value_textbox.configure(state='disabled')
            value_textbox.pack(fill='x', padx=(2, 5), pady=(2))

        # Financial Information Frame
        financial_info_frame = ctk.CTkFrame(profile_container)
        financial_info_frame.grid(row=0,column=2,padx=10,pady=10 ,sticky='nsew')

        ctk.CTkLabel(financial_info_frame ,text="Financial Information" ,font=("Arial" ,14)).pack(anchor='w' ,padx=5)

        financial_info = data.get("Financial_Information", {})
        
        labels_and_keys_financial = [
            ("Credit Score:" ,"Credit_Score") ,
            ("Loans Taken:" ,"Loans_Taken")
        ]

        for label_text ,key in labels_and_keys_financial:
            label = ctk.CTkLabel(financial_info_frame ,text=label_text)
            label.pack(anchor='w' ,padx=(5 ,2) ,pady=(2))

            value_textbox = ctk.CTkTextbox(financial_info_frame ,height=1)
            value_textbox.insert("end" ,str(financial_info.get(key,"N/A")))
            value_textbox.configure(state='disabled')
            value_textbox.pack(fill='x' ,padx=(2 ,5) ,pady=(2))

        # Customer Segment Description Frame...
        segment_description_frame = ctk.CTkFrame(profile_container)
        segment_description_frame.grid(row=1,column=0,columnspan=3,padx=10,pady=(10 ,0) ,sticky='nsew')

        segment_description_label = ctk.CTkLabel(segment_description_frame,text="Customer Segment Description" ,font=("Arial" ,14))
        segment_description_label.pack(anchor='w' ,padx=(5) ,pady=(5))

        segment_description_textbox = Text(segment_description_frame, height=6, wrap='word')
        customer_segment_desc = data.get("Customer_Segment", {}).get("Description", "No description available.")
        segment_description_textbox.insert("end", customer_segment_desc)
        segment_description_textbox.configure(state='disabled')
        segment_description_textbox.pack(expand=True, fill="both", padx=(5), pady=(5))

        # Return Button
        return_button = ctk.CTkButton(profile_container, text="Return", command=self.create_element_profile)
        return_button.grid(row=2,columnspan=3,pady=(10))

    def populate_account_summary(self ,account_data ,transaction_data):
            account_frame = ctk.CTkFrame(self.account_tab)
            account_frame.pack(expand=True ,fill="both" ,padx=10 ,pady=10)

            account_label = ctk.CTkLabel(account_frame ,text="Accounts Summary" ,font=("Arial" ,16))
            account_label.pack(anchor='w' ,pady=(5 ,0) ,padx=5)

            for account in account_data:
                label_text = f"Type: {account['Account Type']}, Balance: {account['Balance']}, Open Date: {account['Open Date']}"
                account_label = ctk.CTkLabel(account_frame,text=label_text)
                account_label.pack(anchor='w' ,pady=(2 ,0) ,padx=5)

            transaction_label = ctk.CTkLabel(account_frame,text="Recent Transactions" ,font=("Arial" ,16))
            transaction_label.pack(anchor='w' ,pady=(10 ,0) ,padx=5)

            transaction_listbox = ctk.CTkTextbox(account_frame)

            for transaction in transaction_data:
                transaction_text = f"{transaction['Date']} - {transaction['Amount']} - {transaction['Description']}"
                transaction_listbox.insert("end" ,transaction_text + "\n")

            transaction_listbox.configure(state='normal')
            
            # Scrollbar for transactions
            transaction_scrollbar = Scrollbar(account_frame)
            transaction_scrollbar.configure(command=transaction_listbox.yview)
            
            transaction_listbox.configure(yscrollcommand=transaction_scrollbar.set)
            
            transaction_listbox.pack(expand=True ,fill="both" ,padx=5,pady=(0 ,10))
            transaction_scrollbar.pack(side='right', fill='y')

            # Return Button
            return_button_account = ctk.CTkButton(account_frame, text="Return", command=self.create_element_profile)
            return_button_account.pack(pady=(10))


if __name__ == "__main__":
    app = MainSystem()
    app.mainloop()