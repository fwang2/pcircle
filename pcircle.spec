%define name pcircle
%define version 0.16
%define unmangled_version 0.16
%define release 1
%define debug_package %{nil}

Autoreq: 0
# turn off auto dependency check
#%define __find_requires %{nil}
#%define __find_provides %{nill}

Summary: A parallel file system tool suite
Name: %{name}
Version: %{version}
Release: %{release}
Source0: %{name}-%{unmangled_version}.tar.gz
License: Apache
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
BuildRequires: python >= 2.7
#BuildRequires: openmpi-devel
#BuildRequires: python-cffi
#BuildRequires: numpy
#BuildRequires: python-scandir
#BuildRequires: libattr-devel
#BuildRequires: pyxattr
#BuildRequires: mpi4py-openmpi
#BuildRequires: lru-dict

#Prefix: %{_prefix}
BuildArch: x86_64
Vendor: Feiyi Wang <fwang2@ornl.gov>
Url: http://github.com/ORNL-TechInt/pcircle

%description
ubiquitous MPI environment in HPC cluster + Work Stealing Pattern +
Distributed Termination Detection = Efficient and Scalable Parallel Solution.
pcircle contains a suite of file system tools that we are developing at OLCF to
take advantage of highly scalable parallel file system such as Lustre and GPFS.
Early tests show very promising scaling properties. However, it is still in
active development, please use it at your own risk. For bug report and
feedbacks, please post it here at https://github.com/olcf/pcircle/issues.



%prep
%setup -n %{name}-%{unmangled_version}
rm -rf %{_topdir}/opt
rm -rf $RPM_BUILD_ROOT

#%build
#python setup.py build

%build
#python setup.py install \
#    --single-version-externally-managed -O1 \
#    --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES
mkdir -p %{_topdir}/opt/%{name}
make VENV=%{_topdir}/opt/%{name} deploy

mkdir -p %{buildroot}
mv %{_topdir}/opt %{buildroot}/ 

#%files -f INSTALLED_FILES
%files
/opt/pcircle
#%defattr(-,root,root)
%post
sed -i -- 's/^VIRTUAL_ENV.*/VIRTUAL_ENV="\/opt\/pcircle"/g' /opt/pcircle/bin/activate
