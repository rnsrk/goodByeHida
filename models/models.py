# models/user.py
from initSchemas import Base
from sqlalchemy import Column, Integer, String

class Artifact(Base):
    __tablename__ = 'w__artifact'
    uri = Column(String)
    f7e2a8a273ab3d577bf5854902550c09 = Column(String) # Document Identifier
    f6e041bd0b16b21596849732c01cb168 = Column(String) # NGK Number
    f17e0296824c2e949f902549bbb9ece4 = Column(String) # Place of Production (Production: be89c84bf729f8e135e2e7b5936f1a44)
    fd06dcc49a29b1a63fa4a789ec17e5c6 = Column(String) # Title
    f35c9c9b0991729c36acb41645fe81d1 = Column(String) # Status
    f2fd7f8a81d5eb1a20371b9acfd1ab59 = Column(String) # Genre
    f05bbd6e29a7d303e4370b04c12b3f75 = Column(String) # Formattribute
    f593fa773a6ea458101ba2325a18abbe = Column(String) # Artifact type
    f476ba24127d4dff1018acebf45a05f6 = Column(String) # Function
    fa7cfd9dbb3d2517c1898b3051d8dbed = Column(String) # Shape
    f8309a21fa79bc6bd2506060b419d2df = Column(String) # Figure
    f3f805d270890837a6493e7e60a96487_height = Column(String)  # Value (Dimension: b73258adf62f35bd1be3fa2863fab558)
    f3f805d270890837a6493e7e60a96487_width = Column(String)  # Value (Dimension: b73258adf62f35bd1be3fa2863fab558),
    f3f805d270890837a6493e7e60a96487_depth = Column(String)  # Value (Dimension: b73258adf62f35bd1be3fa2863fab558),
    f3f805d270890837a6493e7e60a96487_length = Column(String)  # Value (Dimension: b73258adf62f35bd1be3fa2863fab558),
    f3f805d270890837a6493e7e60a96487_diameter = Column(String)  # Value (Dimension: b73258adf62f35bd1be3fa2863fab558),
    f3f805d270890837a6493e7e60a96487_weight = Column(String)  # Value (Dimension: b73258adf62f35bd1be3fa2863fab558),
    f3f805d270890837a6493e7e60a96487_historical_weight = Column(String)  # Value (Dimension: b73258adf62f35bd1be3fa2863fab558),
    f6abbd4f39a6f79de5de2b14b98e51ff = Column(String)  # Keywords
    f26ad2bc1f084478cd7011f7b8451526 = Column(String)  # Description
    f40120d7c13ef02b486c69245f6c2306 = Column(String)  # History
    fd3740649cc06f45677eb0546908cdac = Column(String) # Print Number
    feb10344eaa7a5f414d1e8392853eba9 = Column(String) # Reproduction Number (Image)
    f8976c6a9e5d91fe9caba8a57c27f204 = Column(String) # Change Date (Digitisation Process: b22e6c47ccb3ab8a974b37279e1bc33b)
    fefe289aa0c9563a153be6da7d37e3ff = Column(String) # Comment
    f1f5dd22371e5c1de41e0fb099e0e862 = Column(String)  # Recording Date (Digitisation Process: b22e6c47ccb3ab8a974b37279e1bc33b)
    f78a6310d13c717b82ddba814ac59024 = Column(String)  # Recording note (Digitisation Process: b22e6c47ccb3ab8a974b37279e1bc33b)
    ffb8b04e8d57929a596fc32d6a84d07d = Column(String) # Plugin text
    fee0db94d62fae6370a89ff4757ff539 = Column(String) # Catalogue of Works
    feb48c9a7efc444449b4b8defcd6d8bd = Column(String)  # UUID
    fa613fdf8c591a1ece4cb69eb50a2c2c = Column(String) # Client (Order: b7500363aacae4d77f3838c1fbdfdbc4)
    f6764d6258963038f1e27d5220a86c65 = Column(String)  # Object Assignment
    ff6195ddca1d7c0438b5b6bbe41ffbb5 = Column(String) # Production Data (Production: be89c84bf729f8e135e2e7b5936f1a44)
    f6c31c1a85d9e56988a7039e5e13c7fb = Column(String) # Material
    f45b7c2331355aa88cf4b5af7069d3ba = Column(String) # Mark
    f67eeff97e9d80bdc39d3d26e0b13bcb = Column(String)  # Assignment
    fc7c7d372ed19b8ec158e3a76faf1bf6 = Column(String) # Literature
    f4ac419cded0b30ff267b66c69646606 = Column(String) # Creator (Creation:b59f12befc94b313c9389c498eddd6f1)
    f9d23067d50ec9060904abb1e06db7ab = Column(String) # Administration
    fa135694408c255d6b8f51d61de1bf3e = Column(String) # Artist Activity
    fb44f0fa6c27e1ee03f6a21288272de2 = Column(String) # Production
    f5a3f90d920da3db4cfdbaa6264b0e89 = Column(String) # Digitisation Process

class DigitisationProcess(Base):
    __tablename__ = 'w__digitastion_process'
    f32274ec0032b8778ba69d20108590cc = Column(String(36), primary_key=True) # UUID
    uri = Column(String) # URI
    f8976c6a9e5d91fe9caba8a57c27f204 = Column(String) # Change Date
    f1f5dd22371e5c1de41e0fb099e0e862 = Column(String) # Recording Date
    f78a6310d13c717b82ddba814ac59024 = Column(String) # Recording Note

class Dimension(Base):
    __tablename__ = 'w__dimension'
    f3f805d270890837a6493e7e60a96487 = Column(String, primary_key=True) # UUID
    uri = Column(String)
    f3f805d270890837a6493e7e60a96487_height = Column(String)  # Value
    f3f805d270890837a6493e7e60a96487_width = Column(String)  # Value
    f3f805d270890837a6493e7e60a96487_depth = Column(String)  # Value
    f3f805d270890837a6493e7e60a96487_length = Column(String)  # Value
    f3f805d270890837a6493e7e60a96487_diameter = Column(String)  # Value
    f3f805d270890837a6493e7e60a96487_weight = Column(String)  # Value
    f3f805d270890837a6493e7e60a96487_historical_weight = Column(String)  # Value

class Production(Base):
    __tablename__ = 'w__production'
    f76df4909035e5bd71525a56a1a723b1 = Column(String) # UUID
    uri = Column(String)
    f29f78bb4d883ad7f47810c441df95be = Column(String) # Order
    f17e0296824c2e949f902549bbb9ece4 = Column(String) # Place of Production
    ff6195ddca1d7c0438b5b6bbe41ffbb5 = Column(String) # Production Date

class Order(Base):
    __tablename__ = 'w__order'
    f17c199062692310a45953a5a981da83 = Column(String, primary_key=True) # UUID
    uri = Column(String)
    fa613fdf8c591a1ece4cb69eb50a2c2c = Column(String) # Client
    fb79e3f697e2b2715ac44e023275be30 = Column(String) # Reason
    f79ae1625f8362f94f2a2c7fa75c5a2d = Column(String) # Occupation Status
    f834e2fafb6f2d92fe88a4de0b68ce72 = Column(String) # Note
