Vagrant.configure('2') do |config|
  config.vm.box = 'debian/buster64'
  config.vm.network 'private_network', type: 'dhcp'

  # shared folder does not work with debian, rsync cannot handle the broken
  # symlink in the test directory; just use scp to copy the source tree
  config.vm.synced_folder '.', '/vagrant', disabled: true

  config.vm.provider 'virtualbox' do |vb|
    vb.cpus = 6
    vb.memory = '8192'
  end
  #
  # $ sudo apt-get update
  # $ sudo apt-get -q -y install build-essential curl libgpgme-dev libgpg-error-dev
  # $ sudo apt-get -q -y install clang emacs-nox git
  # $ curl https://sh.rustup.rs -sSf | sh
  # $ source $HOME/.cargo/env
  #
end
