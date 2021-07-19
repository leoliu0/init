# init
load my useful python packages with ease

## Usage

```bash
git clone https://github.com/leoliu0/init.git
cd init
sudo python setup.py install # if you are on linux/Mac
python setup.py install # if you are on Windows
```

Now go to your .ipython startup folder under your user directory
Windows should be in C:\\User\{Your name}\.ipython\profile_default\startup
Linux/Mac should be in /home/{Your name}/.ipython\profile_default\startup

create a py file, say 1.py
and put below line into it
```python
from init import *
```
