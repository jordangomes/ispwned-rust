# Is Pwned Rust
Rust Project for handling the Pwned Password list at https://haveibeenpwned.com/Passwords

Code is bad, spent to much time, use at your own risk

Can use to find out if a individual password, a text file of passwords or a keepassdb has compromised passwords

Individual Password `./is-pwned.exe -p "Pasword123"`

Text File `./is-pwned.exe -f "./passwords.txt"`

Keepass DB `./is-pwned.exe -k "C:\Users\me\Documents\Passwords.kdbx" "KeepassDBPassword"`


## How To Setup
1. Clone and build this project
2. Copy the binaries (is-pwned and split-hashes) to a folder somewhere
3. Download the password hashes from https://haveibeenpwned.com/Passwords and extract the txt file to the same folder as the binaries
4. Rename the txt file to input.txt
5. run split-hashes and wait an hour or 3 (this is spliting the massive file into 65k sepperate files for quick searching)
6. you can now use is-pwned to query the output of the last command
