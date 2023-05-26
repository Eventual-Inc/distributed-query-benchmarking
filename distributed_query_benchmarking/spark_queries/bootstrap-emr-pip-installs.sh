sudo python3 --version

# Install Python 3.8
sudo yum install -y amazon-linux-extras
sudo amazon-linux-extras enable python3.8
sudo yum install -y python3.8

sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.8 99
sudo python3 --version
sudo python3 -m pip install pandas==2.0.1 pyarrow==12.0.0
