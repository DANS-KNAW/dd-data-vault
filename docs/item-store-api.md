About using the Item Store API
===============================

In normal usage the object import interface, described on [the main page](./index.md), is pretty much everything you need. However, there are times when it
may be necessary to edit the layer store on a lower level. As described in the documentation of the
underlying [dans-layer-store-lib]{:target=_blank}, the layer store is conceptually just a file/folder
hierarchy, or **item store**. The service contains API endpoints that let you perform editing operations directly on the item store.

!!! warning "Corruption of the OCFL repository is possible"

    Be aware that, when modifying the item store this way, it is possible to corrupt the OCFL repository. That is why these end-points must be enabled explicitly
    to become available. You can do so by editing the `config.yml` file and restarting the service. It is recommended to reset all the item store endpoints
    to "disabled" after you have finished using them to prevent accidental use.

[dans-layer-store-lib]: https://dans-knaw.github.io/dans-layer-store-lib/#the-itemstore-interface