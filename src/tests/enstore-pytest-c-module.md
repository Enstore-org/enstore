<!-----

Yay, no errors, warnings, or alerts!

Conversion time: 0.485 seconds.


Using this Markdown file:

1. Paste this output into your source file.
2. See the notes and action items below regarding this conversion run.
3. Check the rendered output (headings, lists, code blocks, tables) for proper
   formatting and use a linkchecker before you publish this page.

Conversion notes:

* Docs to Markdown version 1.0β33
* Tue Jan 11 2022 10:46:59 GMT-0800 (PST)
* Source doc: Enstore pytest with C modules
* Tables are currently converted to HTML tables.
----->



#### Install Enstore RPM



* Copy repo details from enstore machine:

```
[enstore@enstore03 ~]$ cat /etc/yum.repos.d/enstore.repo
[enstore]
name=Enstore
baseurl=https://ssasrv1.fnal.gov/en/slf7x/x86_64
enabled=1
gpgcheck=0
priority=85
sslverify=0
```



 ```
[root@fermicloud]# cat <<EOT >> /etc/yum.repos.d/enstore.repo
[enstore]
name=Enstore
baseurl=https://ssasrv1.fnal.gov/en/slf7x/x86_64
enabled=1
gpgcheck=0
priority=85
sslverify=0
EOT
[root@fermicloud]# yum install -y enstore
```




#### Copy setup-enstore script from [setup-enstore Redmine repo](https://cdcvs.fnal.gov/redmine/projects/enstore-config/repository/revisions/master/entry/config/setup-enstore) to your host machine



* This sets the relevant environment variables to use the Python distribution included in the RPM repo
* I just pasted this into `vim`, it will be referred to for the rest of this doc as `tmp/setup-enstore`


#### PIP install pytest (and pytest-mock) as user `enstore`



* This user is set up as part of the enstore RPM install, and owns some of the Python directories included in the package.

 ```
[root@fermicloud]# su enstore
[enstore@fermicloud]$ source /tmp/setup-enstore
<this gives some warnings that are OK>
[enstore@fermicloud]$ python -m pip install pytest pytest-mock
```


* Note - If I try to run `pip` directly I get a `bad interpreter` error. Using `python -m pip` resolves this.


#### Create and run tests (as your dev user)



* Clone git repo

 ```
[root@fermicloud]# su my_user
[my_user@fermicloud]$ cd ~
[my_user@fermicloud ~]$ git clone https://github.com/Enstore-org/enstore.git
```


* Create pytest file and run

 ```
[my_user@fermicloud ~]$ source /tmp/setup-enstore
<this gives some warnings that are OK>
[my_user@fermicloud ~]$ cd enstore/src/
[my_user@fermicloud ~/enstore/src]$ cat <<EOT >> test_enstore_functions2.py
import enstore_functions2

def test_success():
  assert True
EOT
[my_user@fermicloud ~/enstore/src]$ python -m pytest test_enstore_functions2.py
```


* Note: trying to run `pytest` Directly I get `undefined symbol: Py_InitModule4_64` error. Using `python -m pytest` resolves this.
