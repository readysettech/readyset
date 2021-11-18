#! /usr/bin/env zsh

typeset -a general_homebrew_packages
general_homebrew_packages=('tailscale' 'slack' 'zoom' 'firefox' 'google-chrome')
typeset -a dev_homebrew_packages
dev_homebrew_packages=('rustup' 'git' 'homebrew/cask/docker' 'awscli' 'packer' 'docker-compose' 'lz4')
typeset -a ops_homebrew_packages
ops_homebrew_packages=('python' 'terraform' 'rain')

printf "Installing xcode command line tools - watch for an OS prompt"
xcode-select --install

printf "Installing homebrew from https://brew.sh ... "
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

printf "Installing general dependencies"
for package in "${general_homebrew_packages[@]}"
do
  brew install "$package"
done

printf "Installing developer dependencies"
for package in "${dev_homebrew_packages[@]}"
do
  brew install "$package"
done

printf "Installing operations dependencies"
for package in "${ops_homebrew_packages[@]}"
do
  brew install "$package"
done
pip3 install taskcat --user # for CFN testing

printf "
Post-Install Manual Setup:
* All users:
  - Tailscale: VPN Software
    * Click the 'Tailscale' icon the top-right status bar
      -  Looks like nine dots with a 'warning' symbol
    * Select 'Log in' from the menu, which will open a browser window
    * Choose 'Sign in with Google'
  - Zoom: Meeting Video
    * Sign in with Google
    * Options to minimize 'Oops' moments
      - Audio
        * Enable Automatically join computer audio when joining a meeting
        * Enable Mute my mic when joining a meeting
      - Video
        * Enable Stop my video when joining a meeting
  - Set your default browser; Chrome and Firefox should be installed :)
* Developers:
  - Environment Variables
    * `PATH`: Add `/opt/homebrew/bin`
    * `LIBRARY_PATH`: Add `/opt/homebrew/lib`
  - awscli
    * Contact @harley to get a new IAM user set up in AWS
    * Run 'aws configure' to set up your local CLI
"