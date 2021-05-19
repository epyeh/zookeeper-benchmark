cd ~
wget https://repo.anaconda.com/archive/Anaconda3-2021.05-Linux-x86_64.sh
./Anaconda3-2021.05-Linux-x86_64.sh
export PATH="/root/anaconda3/bin:$PATH"

conda create -n zkPython python=3.8
conda activate zkPython
pip install numpy
pip install matplotlib
pip install jupyterlab
export PATH="~/.local/bin:$PATH"