"""
Module documenter extension for Sphinx.
"""

from sphinx.util.compat import Directive

class Automodule2Directive(Directive):
    
    def run(self):
        return []

def setup(app):
    
    app.setup_extension('sphinx.ext.autodoc')
    
    #app.add_node(automodule2)
    app.add_directive('automodule2', Automodule2Directive)
